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

#include "datasystem/common/rdma/ub_port_health.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace datasystem {
namespace {

template <typename Predicate>
bool WaitUntil(Predicate predicate)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

class FakePortStatusProvider : public IUbPortStatusProvider {
public:
    void SetReply(Status status, UbPortHealthSnapshot snapshot = {})
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = std::move(status);
        snapshot_ = snapshot;
    }

    Status QueryPortStatus(UbPortHealthSnapshot &snapshot) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        ++queryCount_;
        if (blockNextQuery_) {
            blockNextQuery_ = false;
            queryBlocked_ = true;
            cv_.notify_all();
            cv_.wait(lock, [this] { return releaseBlockedQuery_; });
            queryBlocked_ = false;
            releaseBlockedQuery_ = false;
        }
        if (status_.IsOk()) {
            snapshot = snapshot_;
        }
        cv_.notify_all();
        return status_;
    }

    void BlockNextQuery()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        blockNextQuery_ = true;
    }

    bool WaitForBlockedQuery()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(1), [this] { return queryBlocked_; });
    }

    void ReleaseBlockedQuery()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        releaseBlockedQuery_ = true;
        cv_.notify_all();
    }

    bool WaitForQueries(size_t count)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(1), [this, count] { return queryCount_ >= count; });
    }

    size_t QueryCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queryCount_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    Status status_;
    UbPortHealthSnapshot snapshot_;
    size_t queryCount_{ 0 };
    bool blockNextQuery_{ false };
    bool queryBlocked_{ false };
    bool releaseBlockedQuery_{ false };
};

TEST(UbPortHealthMonitorTest, AllPortsBadClosesAdmission)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status::OK(), { 4, 4 });
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());

    monitor.TriggerQuery();

    ASSERT_TRUE(provider->WaitForQueries(1));
    EXPECT_TRUE(WaitUntil([&monitor] {
        return monitor.CheckAdmission().GetCode() == K_URMA_WORKER_UNAVAILABLE;
    }));
}

TEST(UbPortHealthMonitorTest, PartialPortFailureKeepsAdmissionOpen)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status::OK(), { 4, 3 });
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());

    monitor.TriggerQuery();

    ASSERT_TRUE(provider->WaitForQueries(1));
    ASSERT_TRUE(WaitUntil([&monitor] { return monitor.GetSnapshot() != nullptr; }));
    EXPECT_TRUE(monitor.CheckAdmission().IsOk());
}

TEST(UbPortHealthMonitorTest, QueryFailureDoesNotCloseAdmission)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status(K_URMA_ERROR, "query failed"));
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());

    monitor.TriggerQuery();

    ASSERT_TRUE(provider->WaitForQueries(1));
    EXPECT_TRUE(monitor.CheckAdmission().IsOk());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(provider->QueryCount(), 1u);
}

TEST(UbPortHealthMonitorTest, InvalidSnapshotDoesNotCloseAdmission)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status::OK(), { 0, 0 });
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());

    monitor.TriggerQuery();

    ASSERT_TRUE(provider->WaitForQueries(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(monitor.CheckAdmission().IsOk());
    EXPECT_EQ(monitor.GetSnapshot(), nullptr);
    EXPECT_EQ(provider->QueryCount(), 1u);
}

TEST(UbPortHealthMonitorTest, QueryFailurePreservesIsolationAndRecoveryPolling)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status::OK(), { 4, 4 });
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());
    monitor.TriggerQuery();
    ASSERT_TRUE(provider->WaitForQueries(1));
    ASSERT_TRUE(WaitUntil([&monitor] {
        return monitor.CheckAdmission().GetCode() == K_URMA_WORKER_UNAVAILABLE;
    }));

    provider->SetReply(Status(K_URMA_ERROR, "query failed"));

    ASSERT_TRUE(provider->WaitForQueries(3));
    EXPECT_EQ(monitor.CheckAdmission().GetCode(), K_URMA_WORKER_UNAVAILABLE);
}

TEST(UbPortHealthMonitorTest, RepeatedTriggersCoalesceToOnePendingQuery)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status::OK(), { 4, 3 });
    provider->BlockNextQuery();
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());
    monitor.TriggerQuery();
    ASSERT_TRUE(provider->WaitForBlockedQuery());

    for (size_t i = 0; i < 32; ++i) {
        monitor.TriggerQuery();
    }
    provider->ReleaseBlockedQuery();

    ASSERT_TRUE(provider->WaitForQueries(2));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(provider->QueryCount(), 2u);
}

TEST(UbPortHealthMonitorTest, StopWaitsForInFlightQueryAndRestartBeginsOpen)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status::OK(), { 4, 4 });
    provider->BlockNextQuery();
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());
    monitor.TriggerQuery();
    ASSERT_TRUE(provider->WaitForBlockedQuery());

    auto stop = std::async(std::launch::async, [&monitor] { monitor.Stop(); });
    EXPECT_EQ(stop.wait_for(std::chrono::milliseconds(20)), std::future_status::timeout);
    provider->ReleaseBlockedQuery();
    EXPECT_EQ(stop.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_TRUE(monitor.CheckAdmission().IsOk());
    EXPECT_EQ(monitor.GetSnapshot(), nullptr);

    ASSERT_TRUE(monitor.Start().IsOk());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(provider->QueryCount(), 1u);
    monitor.Stop();
}

TEST(UbPortHealthMonitorTest, StopClearsPublishedIsolation)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status::OK(), { 4, 4 });
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());
    monitor.TriggerQuery();
    ASSERT_TRUE(WaitUntil([&monitor] {
        return monitor.CheckAdmission().GetCode() == K_URMA_WORKER_UNAVAILABLE;
    }));

    monitor.Stop();

    EXPECT_TRUE(monitor.CheckAdmission().IsOk());
    EXPECT_EQ(monitor.GetSnapshot(), nullptr);
}

TEST(UbPortHealthMonitorTest, PartialRecoveryOpensAdmissionAndPollingStopsOnlyWhenAllGood)
{
    auto provider = std::make_shared<FakePortStatusProvider>();
    provider->SetReply(Status::OK(), { 4, 4 });
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());
    monitor.TriggerQuery();
    ASSERT_TRUE(provider->WaitForQueries(1));
    ASSERT_TRUE(WaitUntil([&monitor] {
        return monitor.CheckAdmission().GetCode() == K_URMA_WORKER_UNAVAILABLE;
    }));

    provider->SetReply(Status::OK(), { 4, 3 });
    ASSERT_TRUE(provider->WaitForQueries(2));
    ASSERT_TRUE(WaitUntil([&monitor] {
        auto snapshot = monitor.GetSnapshot();
        return snapshot != nullptr && snapshot->badPortCount == 3 && monitor.CheckAdmission().IsOk();
    }));
    const auto partialQueryCount = provider->QueryCount();
    ASSERT_TRUE(provider->WaitForQueries(partialQueryCount + 1));

    provider->SetReply(Status::OK(), { 4, 0 });
    ASSERT_TRUE(provider->WaitForQueries(partialQueryCount + 2));
    ASSERT_TRUE(WaitUntil([&monitor] {
        auto snapshot = monitor.GetSnapshot();
        return snapshot != nullptr && snapshot->badPortCount == 0 && monitor.CheckAdmission().IsOk();
    }));
    const auto allGoodQueryCount = provider->QueryCount();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(provider->QueryCount(), allGoodQueryCount);
}

}  // namespace
}  // namespace datasystem
