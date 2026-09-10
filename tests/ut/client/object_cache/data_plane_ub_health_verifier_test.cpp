/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "ut/common.h"
#define private public
#include "datasystem/client/object_cache/transport/data_plane/data_plane_manager.h"
#undef private
#include "datasystem/client/object_cache/routing/ub_health_filter.h"

namespace datasystem::client {
namespace {
const HostPort WORKER("127.0.0.1", 18481);
constexpr char INCARNATION[] = "worker-incarnation";

class FakeVerifierDataPlaneManager final : public DataPlaneManager {
public:
    FakeVerifierDataPlaneManager(UbHealthSummaryApplyHook verifiedHook,
                                 std::function<void()> wakeHook)
        : DataPlaneManager(nullptr, 0, {}, nullptr, false, 1, nullptr, false, true, nullptr, {},
                           std::move(verifiedHook), std::move(wakeHook))
    {
    }

    ~FakeVerifierDataPlaneManager() override { Shutdown(); }

    void RunAndWait()
    {
        RunDueUbPortHealthVerification();
        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while ((ubPortHealthQueryPool_->GetWaitingTasksNum() != 0 ||
                ubPortHealthQueryPool_->GetRunningTasksNum() != 0) && std::chrono::steady_clock::now() < until) {
            std::this_thread::yield();
        }
        EXPECT_EQ(ubPortHealthQueryPool_->GetWaitingTasksNum(), 0u);
        EXPECT_EQ(ubPortHealthQueryPool_->GetRunningTasksNum(), 0u);
    }

    Status QueryUbPortHealth(const HostPort &workerAddr, const std::string &expectedIncarnation,
                             int32_t timeoutMs, UbHealthSummary &summary) override
    {
        ++queryCount;
        EXPECT_EQ(workerAddr, WORKER);
        EXPECT_EQ(expectedIncarnation, INCARNATION);
        EXPECT_EQ(timeoutMs, UB_REMOTE_PORT_HEALTH_QUERY_INTERVAL.count());
        if (queryStatus.IsError()) {
            return queryStatus;
        }
        summary.worker = workerAddr;
        summary.incarnation = expectedIncarnation;
        summary.portHealth = result;
        return Status::OK();
    }

    uint32_t queryCount = 0;
    UbPortHealthSummary result{ true, 4, 4, 1, false };
    Status queryStatus;
};

WorkerSnapshot Snapshot(uint64_t version, bool includeWorker)
{
    WorkerSnapshot snapshot;
    snapshot.ringVersion = version;
    if (includeWorker) {
        snapshot.remoteTransportAddrs.emplace_back(WORKER);
        snapshot.workerIncarnations.emplace(WORKER, INCARNATION);
    }
    return snapshot;
}
}  // namespace

class ControlledVerifierDataPlaneManager final : public DataPlaneManager {
public:
    ControlledVerifierDataPlaneManager() : DataPlaneManager(nullptr, 0, {}, nullptr, false, 1, nullptr, false, true) {}
    ~ControlledVerifierDataPlaneManager() override
    {
        ReleaseAll();
        Shutdown();
    }
    Status QueryUbPortHealth(const HostPort &peer, const std::string &incarnation,
                             int32_t, UbHealthSummary &summary) override
    {
        std::unique_lock<std::mutex> lock(mutex);
        started.push_back(peer);
        changed.notify_all();
        changed.wait(lock, [&] { return released || peer == allowed; });
        summary.worker = peer;
        summary.incarnation = incarnation;
        summary.portHealth = { true, 4, 0, 1, false };
        return Status::OK();
    }
    bool WaitStarted(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex);
        return changed.wait_for(lock, std::chrono::seconds(2), [&] { return started.size() >= count; });
    }
    void ReleaseAll()
    {
        std::lock_guard<std::mutex> lock(mutex);
        released = true;
        changed.notify_all();
    }
    std::mutex mutex;
    std::condition_variable changed;
    std::vector<HostPort> started;
    HostPort allowed;
    bool released = false;
};

TEST(DataPlaneUbHealthVerifierTest, DispatchRefillsFreeSlotAndShutdownDrainsOutstandingQueries)
{
    ControlledVerifierDataPlaneManager manager;
    WorkerSnapshot snapshot;
    snapshot.ringVersion = 1;
    for (int i = 0; i < 5; ++i) {
        HostPort peer("127.0.0.1", 18481 + i);
        snapshot.remoteTransportAddrs.push_back(peer);
        snapshot.workerIncarnations.emplace(peer, INCARNATION);
    }
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());
    for (const auto &peer : snapshot.remoteTransportAddrs) {
        ASSERT_TRUE(manager.RequestUbPortHealthVerification(peer));
    }
    auto dispatch = std::async(std::launch::async, [&] { manager.RunDueUbPortHealthVerification(); });
    const auto dispatchStatus = dispatch.wait_for(std::chrono::seconds(2));
    if (dispatchStatus != std::future_status::ready) {
        manager.ReleaseAll();
        dispatch.get();
    }
    ASSERT_EQ(dispatchStatus, std::future_status::ready);
    EXPECT_TRUE(manager.WaitStarted(4));
    EXPECT_FALSE(manager.GetUbPortHealthQueryDeadline().has_value());
    {
        std::lock_guard<std::mutex> lock(manager.mutex);
        manager.allowed = snapshot.remoteTransportAddrs[1];
        manager.changed.notify_all();
    }
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (manager.ubPortHealthQueriesInFlight_.load() == 4 && std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    manager.RunDueUbPortHealthVerification();
    EXPECT_TRUE(manager.WaitStarted(5));
    EXPECT_EQ(manager.ubPortHealthQueriesInFlight_.load(), 4u);
    snapshot.ringVersion = 2;
    snapshot.workerIncarnations[WORKER] = "replacement-incarnation";
    EXPECT_TRUE(manager.UpdateWorkerSnapshot(snapshot).IsOk());
    EXPECT_TRUE(manager.RequestUbPortHealthVerification(WORKER));
    manager.RunDueUbPortHealthVerification();
    EXPECT_EQ(manager.ubPortHealthQueriesInFlight_.load(), 4u);
    auto shutdown = std::async(std::launch::async, [&] { manager.Shutdown(); });
    EXPECT_EQ(shutdown.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    manager.ReleaseAll();
    dispatch.get();
    shutdown.get();
    EXPECT_EQ(manager.ubPortHealthQueriesInFlight_.load(), 0u);
    EXPECT_FALSE(manager.RequestUbPortHealthVerification(WORKER));
}

TEST(DataPlaneUbHealthVerifierTest, VerifiedAllDownSchedulesOneSecondRecoveryAndRemovalCancelsIt)
{
    std::vector<UbHealthSummary> verified;
    uint32_t wakes = 0;
    FakeVerifierDataPlaneManager manager(
        [&verified](const UbHealthSummary &summary) { verified.emplace_back(summary); },
        [&wakes] { ++wakes; });
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(Snapshot(1, true)).IsOk());

    ASSERT_TRUE(manager.RequestUbPortHealthVerification(WORKER));
    manager.RunAndWait();

    EXPECT_EQ(manager.queryCount, 1u);
    ASSERT_EQ(verified.size(), 1u);
    ASSERT_TRUE(verified.front().portHealth.has_value());
    EXPECT_EQ(verified.front().portHealth->badPortCount, 4u);
    auto deadline = manager.GetUbPortHealthQueryDeadline();
    ASSERT_TRUE(deadline.has_value());
    const auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
        *deadline - std::chrono::steady_clock::now());
    EXPECT_GE(delay.count(), 900);
    EXPECT_LE(delay.count(), UB_REMOTE_PORT_HEALTH_QUERY_INTERVAL.count());
    EXPECT_GE(wakes, 2u);

    ASSERT_TRUE(manager.UpdateWorkerSnapshot(Snapshot(2, false)).IsOk());
    EXPECT_FALSE(manager.GetUbPortHealthQueryDeadline().has_value());
}

TEST(DataPlaneUbHealthVerifierTest, AnyHealthyPortStopsVerification)
{
    std::vector<UbHealthSummary> verified;
    FakeVerifierDataPlaneManager manager(
        [&verified](const UbHealthSummary &summary) { verified.emplace_back(summary); }, {});
    manager.result = { true, 4, 3, 1, false };
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(Snapshot(1, true)).IsOk());

    ASSERT_TRUE(manager.RequestUbPortHealthVerification(WORKER));
    manager.RunAndWait();

    EXPECT_EQ(manager.queryCount, 1u);
    ASSERT_EQ(verified.size(), 1u);
    EXPECT_FALSE(manager.GetUbPortHealthQueryDeadline().has_value());
}

TEST(DataPlaneUbHealthVerifierTest, FirstCqe9WithoutPassiveSummaryRequiresIndependentQuery)
{
    UbHealthFilter filter;
    FakeVerifierDataPlaneManager manager(
        [&filter](const UbHealthSummary &summary) { (void)filter.ApplySummary(summary, INCARNATION); }, {});
    ASSERT_TRUE(manager.UpdateWorkerSnapshot(Snapshot(1, true)).IsOk());
    filter.SetRemotePortHealthVerificationTrigger(
        [&manager](const HostPort &worker) { (void)manager.RequestUbPortHealthVerification(worker); });

    EXPECT_FALSE(filter.ReportWriteTargetFailure(
        WORKER, Status(K_URMA_ERROR, "CQE9 without prior summary"), std::nullopt, URMA_REMOTE_ACK_TIMEOUT_STATUS));
    EXPECT_TRUE(filter.IsAvailable(WORKER));
    EXPECT_TRUE(filter.IsWriteTargetAvailable(WORKER));
    EXPECT_EQ(manager.queryCount, 0u);

    manager.RunAndWait();

    EXPECT_EQ(manager.queryCount, 1u);
    EXPECT_FALSE(filter.IsAvailable(WORKER));
    EXPECT_FALSE(filter.IsWriteTargetAvailable(WORKER));
}

}  // namespace datasystem::client
