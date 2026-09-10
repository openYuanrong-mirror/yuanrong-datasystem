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

#include <gtest/gtest.h>

#include "tests/support/fake_ub_port_status_provider.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/worker/object_cache/worker_self_port_health.h"

namespace datasystem {
namespace object_cache {
namespace {
constexpr std::chrono::milliseconds TEST_QUERY_INTERVAL{ 5 };
const HostPort SELF_WORKER("127.0.0.1", 31501);

using FakePortStatusProvider = ::datasystem::test::FakeUbPortStatusProvider;

class ChangeObserver : public IUbPortHealthObserver {
public:
    void OnUbPortHealthChanged(const UbPortHealthSummary &summary) override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        last = summary;
        ++count;
    }

    size_t Count()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return count;
    }

    UbPortHealthSummary last;
    size_t count = 0;
    std::mutex mutex_;
};

void WaitUntil(const std::function<bool()> &predicate)
{
    constexpr auto kDeadline = std::chrono::milliseconds(2000);
    const auto start = std::chrono::steady_clock::now();
    while (!predicate()) {
        ASSERT_LT(std::chrono::steady_clock::now() - start, kDeadline) << "wait condition timed out";
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

TEST(WorkerSelfPortHealthTest, InitialQueryFailureCannotIsolateBeforeValidFactArrives)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    admission->SetSelfWorker(SELF_WORKER);
    auto provider = std::make_shared<FakePortStatusProvider>(
        std::vector<UbPortStatus>{ { 1, UbPortState::GOOD }, { 2, UbPortState::GOOD } });
    provider->SetResult(Status(K_RUNTIME_ERROR, "fake port status query failed"), {});
    auto binder = std::make_shared<WorkerSelfPortHealth>();
    binder->Attach(admission, SELF_WORKER);
    std::shared_ptr<UbPortHealthMonitor> monitor =
        UbPortHealthMonitor::CreateForTest(provider, TEST_QUERY_INTERVAL);
    ASSERT_EQ(binder->Configure(monitor, {}), Status::OK());
    ASSERT_EQ(monitor->Start(), Status::OK());
    WaitUntil([&] {
        const auto summary = binder->GetSummary();
        return summary.has_value() && summary->verificationPending;
    });

    UbOpOutcome error4(SELF_WORKER, UbOperationKind::CLIENT_GET_WRITEBACK,
                       Status(K_URMA_ERROR, "self port unavailable"));
    error4.cqeStatus = URMA_PORT_UNAVAILABLE_STATUS;
    admission->ReportOutcome(error4);
    ASSERT_TRUE(admission->GetState(SELF_WORKER).has_value());
    EXPECT_EQ(admission->GetState(SELF_WORKER)->state, UbAdmissionState::SUSPECT);
    EXPECT_TRUE(admission->CheckWriteTarget(SELF_WORKER, UbOperationKind::CLIENT_GET_WRITEBACK).IsOk());

    provider->SetResult(Status::OK(), { { 1, UbPortState::GOOD }, { 2, UbPortState::BAD } });
    binder->ReportPortHealthTrigger();
    WaitUntil([&] {
        const auto state = admission->GetState(SELF_WORKER);
        return state.has_value() && state->state == UbAdmissionState::AVAILABLE;
    });
    binder->Stop();
}

TEST(WorkerSelfPortHealthTest, AllDownClosesSelfAdmissionAndPartialRecoveryReopens)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    admission->SetSelfWorker(SELF_WORKER);
    auto provider = std::make_shared<FakePortStatusProvider>(
        std::vector<UbPortStatus>{ { 1, UbPortState::GOOD }, { 2, UbPortState::GOOD } });
    auto observer = std::make_shared<ChangeObserver>();
    auto binder = std::make_shared<WorkerSelfPortHealth>();
    binder->Attach(admission, SELF_WORKER);
    std::shared_ptr<UbPortHealthMonitor> monitor =
        UbPortHealthMonitor::CreateForTest(provider, TEST_QUERY_INTERVAL);
    ASSERT_EQ(binder->Configure(monitor, observer), Status::OK());
    ASSERT_EQ(monitor->Start(), Status::OK());

    provider->SetResult(Status::OK(), { { 1, UbPortState::BAD }, { 2, UbPortState::BAD } });
    binder->ReportPortHealthTrigger();
    WaitUntil([&] {
        const auto summary = binder->GetSummary();
        return summary.has_value() && summary->badPortCount == 2u;
    });
    WaitUntil([&] {
        const auto state = admission->GetState(SELF_WORKER);
        return state.has_value() && state->state == UbAdmissionState::UNAVAILABLE;
    });
    EXPECT_FALSE(admission->BuildSelfHealthSummary(SELF_WORKER).writable);
    ASSERT_TRUE(admission->BuildSelfHealthSummary(SELF_WORKER).portHealth.has_value());
    EXPECT_EQ(admission->BuildSelfHealthSummary(SELF_WORKER).portHealth->badPortCount, 2u);
    EXPECT_EQ(admission->CheckWriteTarget(SELF_WORKER, UbOperationKind::MIGRATION_WRITE).GetCode(),
              K_URMA_WORKER_UNAVAILABLE);

    provider->SetResult(Status::OK(), { { 1, UbPortState::GOOD }, { 2, UbPortState::BAD } });
    binder->ReportPortHealthTrigger();
    WaitUntil([&] {
        const auto state = admission->GetState(SELF_WORKER);
        return state.has_value() && state->state == UbAdmissionState::AVAILABLE;
    });
    EXPECT_TRUE(admission->BuildSelfHealthSummary(SELF_WORKER).writable);
    EXPECT_TRUE(admission->CheckWriteTarget(SELF_WORKER, UbOperationKind::MIGRATION_WRITE).IsOk());
    EXPECT_GT(observer->Count(), 0u);
    binder->Stop();
}

TEST(WorkerSelfPortHealthTest, SelfError4TriggersMonitorRefresh)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    admission->SetSelfWorker(SELF_WORKER);
    auto provider = std::make_shared<FakePortStatusProvider>(
        std::vector<UbPortStatus>{ { 1, UbPortState::GOOD }, { 2, UbPortState::GOOD } });
    auto binder = std::make_shared<WorkerSelfPortHealth>();
    binder->Attach(admission, SELF_WORKER);
    std::shared_ptr<UbPortHealthMonitor> monitor =
        UbPortHealthMonitor::CreateForTest(provider, TEST_QUERY_INTERVAL);
    ASSERT_EQ(binder->Configure(monitor, {}), Status::OK());
    ASSERT_EQ(monitor->Start(), Status::OK());
    WaitUntil([&] {
        const auto summary = binder->GetSummary();
        return summary.has_value() && summary->badPortCount == 0u;
    });
    provider->SetResult(Status::OK(), { { 1, UbPortState::BAD }, { 2, UbPortState::BAD } });
    UbOpOutcome error4(SELF_WORKER, UbOperationKind::CLIENT_GET_WRITEBACK,
                       Status(K_URMA_ERROR, "self port unavailable"));
    error4.cqeStatus = URMA_PORT_UNAVAILABLE_STATUS;

    admission->ReportOutcome(error4);

    WaitUntil([&] {
        const auto state = admission->GetState(SELF_WORKER);
        return state.has_value() && state->state == UbAdmissionState::UNAVAILABLE;
    });
    binder->Stop();
}

}  // namespace
}  // namespace object_cache
}  // namespace datasystem
