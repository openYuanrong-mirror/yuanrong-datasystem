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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "tests/support/fake_ub_port_status_provider.h"

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "tests/ut/worker/object_cache/test_metadata_route.h"
#include "ut/common.h"

namespace datasystem::object_cache {
namespace {
const HostPort SELF("127.0.0.1", 18481);
const HostPort PEER("127.0.0.1", 18482);
constexpr char INCARNATION[] = "self-incarn-0001";
constexpr char PEER_INCARNATION[] = "peer-incarn-0001";

using FakeUbPortStatusProvider = ::datasystem::test::FakeUbPortStatusProvider;

void PublishSelf(cluster::TopologySnapshotState &snapshots)
{
    cluster::TopologyState state;
    state.version = 1;
    state.members = {
        cluster::Member{ { INCARNATION, SELF.ToString() }, cluster::MemberState::ACTIVE, { 1 } },
        cluster::Member{ { PEER_INCARNATION, PEER.ToString() }, cluster::MemberState::ACTIVE, { 2 } }
    };
    std::shared_ptr<const cluster::TopologySnapshot> snapshot;
    ASSERT_TRUE(cluster::TopologySnapshot::Create(
                    std::move(state), 1, std::string(64, 'a'), snapshot)
                    .IsOk());
    cluster::SnapshotUpdateOutcome outcome;
    ASSERT_TRUE(snapshots.Publish(std::move(snapshot), outcome).IsOk());
}

class QueryUbPortHealthTest : public ::datasystem::ut::CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        PublishSelf(snapshots_);
        objectTable_ = std::make_shared<ObjectTable>();
        auto evictionManager = std::make_shared<WorkerOcEvictionManager>(
            objectTable_, SELF, SELF, ::datasystem::ut::GetTestMetadataRoute(), nullptr);
        service_ = std::make_shared<WorkerOCServiceImpl>(
            SELF, SELF, objectTable_, nullptr, evictionManager, nullptr, nullptr, nullptr,
            nullptr, ::datasystem::ut::GetTestMetadataRoute(), membership_, &exitRequested_, false, false);
        provider_ = std::make_shared<FakeUbPortStatusProvider>(
            std::vector<UbPortStatus>{ { 0, UbPortState::GOOD }, { 1, UbPortState::BAD },
                                       { 2, UbPortState::GOOD }, { 3, UbPortState::GOOD } });
    }

    void ConfigurePortHealth()
    {
        std::weak_ptr<IUbPortHealthObserver> observer(service_);
        monitor_ = UbPortHealthMonitor::CreateForTest(provider_, std::chrono::milliseconds(10));
        DS_ASSERT_OK(service_->ConfigureSelfPortHealth(monitor_, observer));
        DS_ASSERT_OK(monitor_->Start());
        (void)monitor_->EnsureFresh(std::chrono::milliseconds(10));
        ASSERT_TRUE(monitor_->GetSummary().has_value());
    }

protected:
    cluster::TopologySnapshotState snapshots_;
    cluster::MembershipEndpointView membership_{ snapshots_ };
    std::atomic<bool> exitRequested_{ false };
    std::shared_ptr<ObjectTable> objectTable_;
    std::shared_ptr<FakeUbPortStatusProvider> provider_;
    std::shared_ptr<UbPortHealthMonitor> monitor_;
    std::shared_ptr<WorkerOCServiceImpl> service_;
};
}  // namespace

TEST_F(QueryUbPortHealthTest, ReturnsCachedAggregateWithCurrentWorkerIdentity)
{
    ConfigurePortHealth();
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation(INCARNATION);
    QueryUbPortHealthRspPb response;

    DS_ASSERT_OK(service_->QuerySelfUbPortHealth(request, response));

    EXPECT_GE(provider_->Calls(), 1u);
    ASSERT_TRUE(response.has_health_summary());
    UbHealthSummary summary;
    DS_ASSERT_OK(DecodeUbHealthSummary(response.health_summary(), summary));
    EXPECT_EQ(summary.worker, SELF);
    EXPECT_EQ(summary.incarnation, INCARNATION);
    ASSERT_TRUE(summary.portHealth.has_value());
    EXPECT_EQ(summary.portHealth->totalPortCount, 4u);
    EXPECT_EQ(summary.portHealth->badPortCount, 1u);
    EXPECT_EQ(summary.portHealth->healthEpoch, UB_PORT_HEALTH_FIRST_EPOCH);
}

TEST_F(QueryUbPortHealthTest, RejectsStaleIncarnationBeforeCallingQuery)
{
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation("retired-incarnation");
    QueryUbPortHealthRspPb response;

    auto status = service_->QuerySelfUbPortHealth(request, response);

    EXPECT_EQ(status.GetCode(), K_NOT_READY);
    EXPECT_EQ(provider_->Calls(), 0u);
    EXPECT_FALSE(response.has_health_summary());
}

TEST_F(QueryUbPortHealthTest, QueryDoesNotWaitForBlockedProvider)
{
    provider_->Pause();
    monitor_ = UbPortHealthMonitor::CreateForTest(provider_, std::chrono::seconds(1));
    DS_ASSERT_OK(service_->ConfigureSelfPortHealth(monitor_));
    DS_ASSERT_OK(monitor_->Start());
    ASSERT_TRUE(provider_->WaitForCalls(1));
    QueryUbPortHealthReqPb request;
    request.set_expected_worker_incarnation(INCARNATION);
    QueryUbPortHealthRspPb response;
    auto query = std::async(std::launch::async, [&] { return service_->QuerySelfUbPortHealth(request, response); });
    const auto ready = query.wait_for(std::chrono::milliseconds(200));
    provider_->Resume();
    EXPECT_EQ(ready, std::future_status::ready);
    EXPECT_EQ(query.get().GetCode(), K_NOT_READY);
    EXPECT_FALSE(response.has_health_summary());
}

}  // namespace datasystem::object_cache
