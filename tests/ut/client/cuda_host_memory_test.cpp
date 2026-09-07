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
#include <stdexcept>

#include <gtest/gtest.h>

namespace datasystem {
namespace {
std::atomic<int> firstHostRegisterCalls{ 0 };
std::atomic<int> firstHostUnregisterCalls{ 0 };
std::atomic<int> firstMemcpyCalls{ 0 };
std::atomic<int> secondHostRegisterCalls{ 0 };
std::atomic<int> secondHostUnregisterCalls{ 0 };
std::atomic<int> secondGetErrorStringCalls{ 0 };
std::atomic<int> secondMemcpyCalls{ 0 };

int FirstHostRegister(void *, size_t, unsigned int)
{
    ++firstHostRegisterCalls;
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
}  // namespace

TEST(CudaHostMemoryTest, FirstValidRegistrationIsFrozenAndCallbackExceptionDoesNotEscape)
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
}
}  // namespace datasystem
