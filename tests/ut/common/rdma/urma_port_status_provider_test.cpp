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

#include <cstdint>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/rdma/urma_port_status_provider.h"

#if defined(USE_URMA) || defined(USE_URMA_MOCK)
#ifdef USE_URMA_MOCK
#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#include "datasystem/common/rdma/urma_dlopen_util.h"
#else
#include <ub/umdk/urma/urma_api.h>
#include <ub/umdk/urma/urma_ubagg.h>
#endif

namespace datasystem {
namespace {
constexpr char MOCK_PORT_STATUS[] = "UrmaMock.QueryPortStatus";
constexpr char MOCK_PORT_STATUS_DUPLICATE[] = "UrmaMock.QueryPortStatus.duplicate";
constexpr char MOCK_PORT_STATUS_ERROR[] = "UrmaMock.QueryPortStatus.error";
constexpr char MOCK_PORT_STATUS_INVALID_COUNT[] = "UrmaMock.QueryPortStatus.invalidCount";
constexpr char MOCK_PORT_STATUS_SHORT_OUTPUT[] = "UrmaMock.QueryPortStatus.shortOutput";
constexpr char MOCK_PORT_STATUS_UNKNOWN[] = "UrmaMock.QueryPortStatus.unknown";

void ExpectOutputUnchanged(const std::vector<UbPortStatus> &portStatus)
{
    ASSERT_EQ(portStatus.size(), 1u);
    EXPECT_EQ(portStatus.front().portIndex, 99u);
    EXPECT_EQ(portStatus.front().state, UbPortState::GOOD);
}

TEST(UrmaPortStatusProviderTest, NullContextIsRejectedWithoutChangingOutput)
{
    UrmaPortStatusProvider provider(nullptr);
    std::vector<UbPortStatus> portStatus{ { 99, UbPortState::GOOD } };

    EXPECT_EQ(provider.QueryPortStatus(portStatus).GetCode(), K_INVALID);
    ExpectOutputUnchanged(portStatus);
}

#ifdef USE_URMA_MOCK
class UrmaPortStatusProviderMatrixTest : public ::testing::TestWithParam<uint32_t> {
public:
    void TearDown() override
    {
        (void)inject::Clear(MOCK_PORT_STATUS);
        (void)inject::Clear(MOCK_PORT_STATUS_DUPLICATE);
        (void)inject::Clear(MOCK_PORT_STATUS_ERROR);
        (void)inject::Clear(MOCK_PORT_STATUS_INVALID_COUNT);
        (void)inject::Clear(MOCK_PORT_STATUS_SHORT_OUTPUT);
        (void)inject::Clear(MOCK_PORT_STATUS_UNKNOWN);
    }
};

TEST_P(UrmaPortStatusProviderMatrixTest, ConvertsMultiChipOutputWithChipLocalPortIndexes)
{
    const uint32_t badPortCount = GetParam();
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS, "call(4," + std::to_string(badPortCount) + ")").IsOk());
    UrmaPortStatusProvider provider(reinterpret_cast<void *>(1));
    std::vector<UbPortStatus> portStatus{ { 99, UbPortState::GOOD } };

    ASSERT_TRUE(provider.QueryPortStatus(portStatus).IsOk());

    ASSERT_EQ(portStatus.size(), 4u);
    uint32_t observedBad = 0;
    for (uint32_t i = 0; i < portStatus.size(); ++i) {
        EXPECT_EQ(portStatus[i].portIndex, i);
        observedBad += portStatus[i].state == UbPortState::BAD ? 1u : 0u;
    }
    EXPECT_EQ(observedBad, badPortCount);
}

INSTANTIATE_TEST_SUITE_P(AllPortHealthCombinations, UrmaPortStatusProviderMatrixTest,
                         ::testing::Values(0u, 1u, 2u, 3u, 4u));

TEST_F(UrmaPortStatusProviderMatrixTest, UserCtlFailureKeepsCallerOutputUntouched)
{
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS_ERROR, "return()").IsOk());
    UrmaPortStatusProvider provider(reinterpret_cast<void *>(1));
    std::vector<UbPortStatus> portStatus{ { 99, UbPortState::GOOD } };

    EXPECT_EQ(provider.QueryPortStatus(portStatus).GetCode(), K_URMA_ERROR);
    ExpectOutputUnchanged(portStatus);
}

TEST_F(UrmaPortStatusProviderMatrixTest, EmptyPortSetIsRejectedWithoutChangingOutput)
{
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS, "call(0,0)").IsOk());
    UrmaPortStatusProvider provider(reinterpret_cast<void *>(1));
    std::vector<UbPortStatus> portStatus{ { 99, UbPortState::GOOD } };

    EXPECT_EQ(provider.QueryPortStatus(portStatus).GetCode(), K_INVALID);
    ExpectOutputUnchanged(portStatus);
}

TEST_F(UrmaPortStatusProviderMatrixTest, InvalidPortCountIsRejectedWithoutChangingOutput)
{
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS_INVALID_COUNT, "call()").IsOk());
    UrmaPortStatusProvider provider(reinterpret_cast<void *>(1));
    std::vector<UbPortStatus> portStatus{ { 99, UbPortState::GOOD } };

    EXPECT_EQ(provider.QueryPortStatus(portStatus).GetCode(), K_INVALID);
    ExpectOutputUnchanged(portStatus);
}

TEST_F(UrmaPortStatusProviderMatrixTest, DuplicatePortIdentityIsRejectedWithoutChangingOutput)
{
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS_DUPLICATE, "call()").IsOk());
    UrmaPortStatusProvider provider(reinterpret_cast<void *>(1));
    std::vector<UbPortStatus> portStatus{ { 99, UbPortState::GOOD } };

    EXPECT_EQ(provider.QueryPortStatus(portStatus).GetCode(), K_INVALID);
    ExpectOutputUnchanged(portStatus);
}

TEST_F(UrmaPortStatusProviderMatrixTest, ShortOutputIsRejectedWithoutChangingOutput)
{
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS_SHORT_OUTPUT, "call()").IsOk());
    UrmaPortStatusProvider provider(reinterpret_cast<void *>(1));
    std::vector<UbPortStatus> portStatus{ { 99, UbPortState::GOOD } };

    EXPECT_EQ(provider.QueryPortStatus(portStatus).GetCode(), K_INVALID);
    ExpectOutputUnchanged(portStatus);
}

TEST_F(UrmaPortStatusProviderMatrixTest, UnknownPortStateIsRejectedWithoutChangingOutput)
{
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS_UNKNOWN, "call()").IsOk());
    UrmaPortStatusProvider provider(reinterpret_cast<void *>(1));
    std::vector<UbPortStatus> portStatus{ { 99, UbPortState::GOOD } };

    EXPECT_EQ(provider.QueryPortStatus(portStatus).GetCode(), K_INVALID);
    ExpectOutputUnchanged(portStatus);
}

class UrmaPortStatusProviderLiveMockTest : public testing::Test {
protected:
    void SetUp() override
    {
        urma_dlopen::Cleanup();
        ASSERT_TRUE(urma_dlopen::Init());
        urma_init_attr_t attr{};
        ASSERT_EQ(ds_urma_init(&attr), URMA_SUCCESS);
        initialized_ = true;
        int deviceCount = 0;
        auto **devices = ds_urma_get_device_list(&deviceCount);
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

    bool WaitForHealth(UbPortHealthMonitor &monitor, uint32_t bad)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        do {
            auto snapshot = monitor.GetSnapshot();
            if (snapshot != nullptr && snapshot->valid && !snapshot->verificationPending
                && snapshot->badPortCount == bad) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    }

    urma_context_t *context_{ nullptr };
    bool initialized_{ false };
};

TEST_F(UrmaPortStatusProviderLiveMockTest, ReadsMockUserCtlPortCountMatrix)
{
    UrmaPortStatusProvider provider(context_);
    for (uint32_t badPortCount = 0; badPortCount <= 4; ++badPortCount) {
        ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS, "call(4," + std::to_string(badPortCount) + ")").IsOk());
        std::vector<UbPortStatus> ports;
        ASSERT_TRUE(provider.QueryPortStatus(ports).IsOk());
        ASSERT_EQ(ports.size(), 4u);
        for (size_t index = 0; index < ports.size(); ++index) {
            EXPECT_EQ(ports[index].state, index < badPortCount ? UbPortState::BAD : UbPortState::GOOD);
        }
    }
}

TEST_F(UrmaPortStatusProviderLiveMockTest, QueryFailurePreservesCallerSnapshot)
{
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS_ERROR, "return(K_URMA_ERROR)").IsOk());
    UrmaPortStatusProvider provider(context_);
    std::vector<UbPortStatus> ports{ { 99, UbPortState::GOOD } };
    EXPECT_EQ(provider.QueryPortStatus(ports).GetCode(), K_URMA_ERROR);
    ExpectOutputUnchanged(ports);
}

TEST_F(UrmaPortStatusProviderLiveMockTest, MockUserCtlDrivesMonitorIsolationAndPartialRecovery)
{
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS, "call(4,4)").IsOk());
    auto provider = std::make_shared<UrmaPortStatusProvider>(context_);
    auto monitor = UbPortHealthMonitor::CreateForTest(provider, std::chrono::milliseconds(20));
    ASSERT_TRUE(monitor->Start().IsOk());
    ASSERT_TRUE(WaitForHealth(*monitor, 4));
    EXPECT_TRUE(IsLocalUbNodeIsolated(monitor->GetSnapshot()));
    ASSERT_TRUE(inject::Set(MOCK_PORT_STATUS, "call(4,3)").IsOk());
    ASSERT_TRUE(WaitForHealth(*monitor, 3));
    EXPECT_FALSE(IsLocalUbNodeIsolated(monitor->GetSnapshot()));
}
#endif

}  // namespace
}  // namespace datasystem
#endif
