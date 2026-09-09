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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#ifdef USE_URMA_MOCK
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"
#include "datasystem/common/rdma/urma_port_status_provider.h"
#endif

namespace datasystem {
namespace {

#ifdef USE_URMA_MOCK

template <typename Predicate>
bool WaitUntil(Predicate predicate)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

class UrmaPortStatusProviderTest : public testing::Test {
protected:
    void SetUp() override
    {
        urma_dlopen::Cleanup();
        ASSERT_TRUE(urma_dlopen::Init());
        urma_init_attr_t attr{};
        ASSERT_EQ(ds_urma_init(&attr), URMA_SUCCESS);
        initialized_ = true;
        int deviceCount = 0;
        urma_device_t **devices = ds_urma_get_device_list(&deviceCount);
        ASSERT_NE(devices, nullptr);
        ASSERT_GT(deviceCount, 0);
        context_ = ds_urma_create_context(devices[0], 0);
        delete[] devices;
        ASSERT_NE(context_, nullptr);
    }

    void TearDown() override
    {
        (void)inject::ClearAll();
        if (context_ != nullptr) {
            (void)ds_urma_delete_context(context_);
        }
        if (initialized_) {
            (void)ds_urma_uninit();
        }
        urma_dlopen::Cleanup();
    }

    UrmaPortStatusProvider MakeProvider()
    {
        return UrmaPortStatusProvider(context_);
    }

    urma_context_t *context_{ nullptr };
    bool initialized_{ false };
};

TEST_F(UrmaPortStatusProviderTest, ReadsMultiChipPortsWithChipLocalIndexes)
{
    auto provider = MakeProvider();
    for (uint32_t badPortCount = 0; badPortCount <= 4; ++badPortCount) {
        const std::string action = "call(4," + std::to_string(badPortCount) + ")";
        ASSERT_TRUE(inject::Set("UrmaMock.QueryPortStatus", action).IsOk());
        UbPortHealthSnapshot snapshot;

        ASSERT_TRUE(provider.QueryPortStatus(snapshot).IsOk());

        EXPECT_EQ(snapshot.totalPortCount, 4u);
        EXPECT_EQ(snapshot.badPortCount, badPortCount);
    }
}

TEST_F(UrmaPortStatusProviderTest, QueryFailurePreservesCallerSnapshot)
{
    ASSERT_TRUE(inject::Set("UrmaMock.QueryPortStatus.error", "return(K_URMA_ERROR)").IsOk());
    auto provider = MakeProvider();
    UbPortHealthSnapshot snapshot{ 8, 2 };

    EXPECT_EQ(provider.QueryPortStatus(snapshot).GetCode(), K_URMA_ERROR);

    EXPECT_EQ(snapshot.totalPortCount, 8u);
    EXPECT_EQ(snapshot.badPortCount, 2u);
}

TEST_F(UrmaPortStatusProviderTest, MockUserCtlDrivesMonitorIsolationAndPartialRecovery)
{
    ASSERT_TRUE(inject::Set("UrmaMock.QueryPortStatus", "call(4,4)").IsOk());
    auto provider = std::make_shared<UrmaPortStatusProvider>(context_);
    UbPortHealthMonitor monitor(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor.Start().IsOk());
    monitor.TriggerQuery();
    ASSERT_TRUE(WaitUntil([&monitor] {
        return monitor.CheckAdmission().GetCode() == K_URMA_WORKER_UNAVAILABLE;
    }));

    ASSERT_TRUE(inject::Set("UrmaMock.QueryPortStatus", "call(4,3)").IsOk());

    ASSERT_TRUE(WaitUntil([&monitor] {
        auto snapshot = monitor.GetSnapshot();
        return snapshot != nullptr && snapshot->badPortCount == 3 && monitor.CheckAdmission().IsOk();
    }));
}

#else

TEST(UrmaPortStatusProviderTest, RequiresUrmaMock)
{
    GTEST_SKIP() << "URMA port-status provider unit tests require USE_URMA_MOCK";
}

#endif

}  // namespace
}  // namespace datasystem
