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

/** Description: Test process-wide CUDA callback registration. */
#include "datasystem/common/device/nvidia/cuda_host_memory.h"

#include <atomic>
#include <memory>
#include <stdexcept>
#include <vector>

#ifdef __linux__
#include <linux/memfd.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include <gtest/gtest.h>

#include "datasystem/client/mmap_manager/shm_mmap_table_entry.h"

namespace datasystem {
namespace {
#ifdef __linux__
constexpr size_t PIN_SLICE_SIZE = 64UL * 1024UL * 1024UL;
constexpr size_t EXPECTED_PIN_FRAGMENT_COUNT = 2;
#endif
std::atomic<int> firstHostRegisterCalls{ 0 };
std::atomic<int> firstHostUnregisterCalls{ 0 };
std::atomic<int> firstMemcpyCalls{ 0 };
std::atomic<int> secondHostRegisterCalls{ 0 };
std::atomic<int> secondHostUnregisterCalls{ 0 };
std::atomic<int> secondGetErrorStringCalls{ 0 };
std::atomic<int> secondMemcpyCalls{ 0 };
std::atomic<bool> markClientExitingOnRegister{ false };
const std::shared_ptr<std::atomic<bool>> clientExiting = std::make_shared<std::atomic<bool>>(false);

int FirstHostRegister(void *, size_t, unsigned int)
{
    ++firstHostRegisterCalls;
    if (markClientExitingOnRegister.load(std::memory_order_acquire)) {
        clientExiting->store(true, std::memory_order_release);
    }
    return kCudaSuccess;
}

int FirstHostUnregister(void *)
{
    ++firstHostUnregisterCalls;
    return kCudaSuccess;
}

int FirstMemcpyAsync(void *, const void *, size_t, DsCudaMemcpyKind, void *)
{
    ++firstMemcpyCalls;
    return 17;
}

const char *ThrowingGetErrorString(int)
{
    throw std::runtime_error("injected getErrorString failure");
}

int SecondHostRegister(void *, size_t, unsigned int)
{
    ++secondHostRegisterCalls;
    return kCudaSuccess;
}

int SecondHostUnregister(void *)
{
    ++secondHostUnregisterCalls;
    return kCudaSuccess;
}

const char *SecondGetErrorString(int)
{
    ++secondGetErrorStringCalls;
    return "second CUDA error";
}

int SecondMemcpyAsync(void *, const void *, size_t, DsCudaMemcpyKind, void *)
{
    ++secondMemcpyCalls;
    return 18;
}

#ifdef __linux__
void VerifyClientExitStopsRemainingPinFragments()
{
    const size_t mmapSize = PIN_SLICE_SIZE + static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const int fd = static_cast<int>(syscall(SYS_memfd_create, "mmap_entry_exit_ut", MFD_ALLOW_SEALING));
    ASSERT_GE(fd, 0);
    ASSERT_EQ(ftruncate(fd, static_cast<off_t>(mmapSize)), 0);
    firstHostRegisterCalls.store(0, std::memory_order_release);
    firstHostUnregisterCalls.store(0, std::memory_order_release);
    clientExiting->store(false, std::memory_order_release);
    markClientExitingOnRegister.store(true, std::memory_order_release);

    {
        client::ShmMmapTableEntry entry(fd, mmapSize);
        ASSERT_TRUE(entry.Init(false, "").IsOk());
        std::vector<size_t> segmentSizes;
        ASSERT_TRUE(entry.GetMemcpySegmentSizes(entry.Pointer(), mmapSize, segmentSizes).IsOk());
        ASSERT_EQ(segmentSizes.size(), EXPECTED_PIN_FRAGMENT_COUNT);
        entry.SetClientExitingFlag(clientExiting);
        entry.PinHostMemory();

        EXPECT_TRUE(entry.IsCudaHostMemoryRegistrationDone());
        EXPECT_EQ(firstHostRegisterCalls.load(std::memory_order_acquire), 1);
    }
    EXPECT_EQ(firstHostUnregisterCalls.load(std::memory_order_acquire), 1);
    markClientExitingOnRegister.store(false, std::memory_order_release);
}

#endif
}  // namespace

TEST(CudaHostMemoryTest, RegisteredCallbacksAreFrozenAndClientExitControlsPinFragments)
{
    char source = 0;
    char destination = 0;
    RegisterCudaFuncs({});

    CudaFuncs incompleteFuncs;
    incompleteFuncs.hostRegister = SecondHostRegister;
    incompleteFuncs.hostUnregister = SecondHostUnregister;
    RegisterCudaFuncs(incompleteFuncs);

    CudaFuncs firstFuncs;
    firstFuncs.hostRegister = FirstHostRegister;
    firstFuncs.hostUnregister = FirstHostUnregister;
    firstFuncs.getErrorString = ThrowingGetErrorString;
    firstFuncs.memcpyAsync = FirstMemcpyAsync;
    RegisterCudaFuncs(firstFuncs);

    CudaFuncs secondFuncs;
    secondFuncs.hostRegister = SecondHostRegister;
    secondFuncs.hostUnregister = SecondHostUnregister;
    secondFuncs.getErrorString = SecondGetErrorString;
    secondFuncs.memcpyAsync = SecondMemcpyAsync;
    RegisterCudaFuncs(secondFuncs);

    EXPECT_TRUE(RegisterCudaHostMemory(&source, sizeof(source)));
    EXPECT_TRUE(UnregisterCudaHostMemory(&source));
    const auto status =
        DsCudaMemcpyAsync(&destination, &source, sizeof(source), DsCudaMemcpyKind::HOST_TO_DEVICE, nullptr);

    EXPECT_EQ(firstHostRegisterCalls.load(), 1);
    EXPECT_EQ(firstHostUnregisterCalls.load(), 1);
    EXPECT_EQ(firstMemcpyCalls.load(), 1);
    EXPECT_EQ(secondHostRegisterCalls.load(), 0);
    EXPECT_EQ(secondHostUnregisterCalls.load(), 0);
    EXPECT_EQ(secondGetErrorStringCalls.load(), 0);
    EXPECT_EQ(secondMemcpyCalls.load(), 0);
    EXPECT_EQ(status.GetCode(), K_RUNTIME_ERROR);
    EXPECT_NE(status.ToString().find("error: 17"), std::string::npos);

#ifdef __linux__
    VerifyClientExitStopsRemainingPinFragments();
#endif
}
}  // namespace datasystem
