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

/**
 * Description: Client mmap table management.
 */
#include "datasystem/client/mmap_manager/shm_mmap_table_entry.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <chrono>
#include <exception>
#include <shared_mutex>
#include <thread>
#include <sys/mman.h>
#include <unistd.h>

#include "datasystem/common/device/nvidia/cuda_host_memory.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace client {
namespace {
constexpr auto HOST_MEMORY_FRAGMENT_INTERVAL = std::chrono::milliseconds(5);
constexpr size_t HOST_MEMORY_FRAGMENT_SIZE = 64UL * 1024UL * 1024UL;
constexpr size_t HOST_MEMORY_PIN_MAX_RETRY_COUNT = 3;
}  // namespace

Status ShmMmapTableEntry::Init(bool enableHugeTlb, const std::string &tenantId)
{
    (void)tenantId;
    std::stringstream err;
    if (size_ <= 0) {
        err << "The mmap size [" << size_ << "] is invalid for fd [" << fd_ << "]";
        LOG(ERROR) << err.str();
        RETURN_STATUS(StatusCode::K_INVALID, err.str());
    }
    INJECT_POINT("IMmapTableEntry.mmap");
    // mmap fd
    uint32_t mFlag = MAP_SHARED;
    if (enableHugeTlb) {
        mFlag |= MAP_HUGETLB;
    }
    pointer_ = reinterpret_cast<uint8_t *>(mmap(nullptr, size_, PROT_READ | PROT_WRITE, mFlag, fd_, 0));
    if (pointer_ == MAP_FAILED) {
        RETURN_STATUS_LOG_ERROR(
            StatusCode::K_RUNTIME_ERROR,
            FormatString("Mmap [client id = %s, fd = %d] failed. Error no: [%s]", clientId_, fd_, StrErr(errno)));
    }
    // Exclude the shared memory from core dump.
    int ret = madvise(pointer_, size_, MADV_DONTDUMP);
    if (ret != 0) {
        // Ignore and write log.
        LOG(WARNING) << "madvise DONTDUMP memory failed: " << StrErr(errno);
    }
    // Closing this fd has an effect on performance.
    RETRY_ON_EINTR(close(fd_));
    LOG(INFO) << FormatString("mmap success, client id: %s, fd: %d, size: %zu", clientId_, fd_, size_);
    BuildPinRange();
    return Status::OK();
}

void ShmMmapTableEntry::BuildPinRange()
{
    pinRange_ = PinRange{ pointer_, size_, HOST_MEMORY_FRAGMENT_SIZE };
}

size_t ShmMmapTableEntry::GetPinFragmentCount() const
{
    if (pinRange_.sliceSize == 0) {
        return 0;
    }
    return pinRange_.totalSize / pinRange_.sliceSize
           + (pinRange_.totalSize % pinRange_.sliceSize == 0 ? 0 : 1);
}

ShmMmapTableEntry::PinFragment ShmMmapTableEntry::GetPinFragment(size_t fragmentIndex) const
{
    const size_t offset = fragmentIndex * pinRange_.sliceSize;
    return PinFragment{ pinRange_.startAddr + offset, std::min(pinRange_.sliceSize, pinRange_.totalSize - offset) };
}

void ShmMmapTableEntry::PinHostMemory()
{
    std::lock_guard<std::mutex> lock(*hostMemoryOperationMutex_);
    const bool registrationEnabled = IsCudaHostMemoryRegistrationEnabled();
    const auto begin = std::chrono::steady_clock::now();
    const size_t fragmentCount = GetPinFragmentCount();
    LOG(INFO) << "[CudaHostMemory] Worker shared memory pin started, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << fragmentCount
              << ", fragmentIntervalMs: " << HOST_MEMORY_FRAGMENT_INTERVAL.count()
              << ", registrationEnabled: " << registrationEnabled;
    try {
        INJECT_POINT_NO_RETURN("ShmMmapTableEntry.PinHostMemory");
    } catch (const std::exception &e) {
        LOG(WARNING) << "CUDA host memory pin injection failed: " << e.what();
    } catch (...) {
        LOG(WARNING) << "CUDA host memory pin injection failed with an unknown exception";
    }
    if (!registrationEnabled) {
        (void)RegisterCudaHostMemory(pointer_, size_);
        pinCompleted_.store(true, std::memory_order_release);
        const auto elapsedUs =
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
        LOG(INFO) << "[CudaHostMemory] Worker shared memory pin finished, clientId: " << clientId_
                  << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
                  << ", fragmentCount: " << fragmentCount
                  << ", attemptedCount: 0, successCount: 0, failedCount: 0"
                  << ", registrationEnabled: false, completed: true, elapsedUs: " << elapsedUs.count();
        return;
    }
    pinAttempted_.store(true, std::memory_order_release);
    const auto pinResult = PinHostMemoryFragments();
    pinnedFragmentCount_.store(pinResult.successCount, std::memory_order_release);
    pinCompleted_.store(true, std::memory_order_release);
    const size_t failedCount = pinResult.attemptedFragmentCount - pinResult.successCount;
    const auto elapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
    LOG(INFO) << "[CudaHostMemory] Worker shared memory pin finished, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << fragmentCount
              << ", attemptedCount: " << pinResult.attemptedFragmentCount
              << ", successCount: " << pinResult.successCount << ", failedCount: " << failedCount
              << ", retryCount: " << pinResult.retryCount
              << ", stoppedByClientExit: " << pinResult.stoppedByClientExit
              << ", registrationEnabled: true, completed: true, elapsedUs: " << elapsedUs.count();
}

ShmMmapTableEntry::PinResult ShmMmapTableEntry::PinHostMemoryFragments()
{
    PinResult result;
    size_t remainingRetryCount = HOST_MEMORY_PIN_MAX_RETRY_COUNT;
    const size_t fragmentCount = GetPinFragmentCount();
    for (size_t i = 0; i < fragmentCount; ++i) {
        if (IsClientExiting()) {
            result.stoppedByClientExit = true;
            break;
        }
        if (i > 0) {
            std::this_thread::sleep_for(HOST_MEMORY_FRAGMENT_INTERVAL);
            if (IsClientExiting()) {
                result.stoppedByClientExit = true;
                break;
            }
        }
        ++result.attemptedFragmentCount;
        while (!PinHostMemoryFragment(i)) {
            if (IsClientExiting()) {
                result.stoppedByClientExit = true;
                return result;
            }
            if (remainingRetryCount == 0) {
                const auto fragment = GetPinFragment(i);
                LOG(ERROR) << "[CudaHostMemory] Worker shared memory pin stopped after retries were exhausted, "
                           << "clientId: " << clientId_ << ", fragmentIndex: " << i
                           << ", pointer: " << static_cast<void *>(fragment.pointer)
                           << ", size: " << fragment.size << ", successCount: " << result.successCount;
                return result;
            }
            --remainingRetryCount;
            ++result.retryCount;
            LOG(WARNING) << "[CudaHostMemory] Retry Worker shared memory fragment pin, clientId: " << clientId_
                         << ", fragmentIndex: " << i << ", retryCount: " << result.retryCount
                         << ", remainingRetryCount: " << remainingRetryCount;
        }
        ++result.successCount;
    }
    return result;
}

bool ShmMmapTableEntry::IsClientExiting() const
{
    return clientExiting_ != nullptr && clientExiting_->load(std::memory_order_acquire);
}

bool ShmMmapTableEntry::PinHostMemoryFragment(size_t fragmentIndex)
{
    const auto fragment = GetPinFragment(fragmentIndex);
    try {
        return RegisterCudaHostMemory(fragment.pointer, fragment.size);
    } catch (const std::exception &e) {
        LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment pin failed unexpectedly, clientId: "
                   << clientId_ << ", fragmentIndex: " << fragmentIndex
                   << ", pointer: " << static_cast<void *>(fragment.pointer)
                   << ", size: " << fragment.size << ", error: " << e.what();
    } catch (...) {
        LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment pin failed with an unknown exception, clientId: "
                   << clientId_ << ", fragmentIndex: " << fragmentIndex
                   << ", pointer: " << static_cast<void *>(fragment.pointer) << ", size: " << fragment.size;
    }
    return false;
}

void ShmMmapTableEntry::SkipHostMemoryPin()
{
    pinCompleted_.store(true, std::memory_order_release);
}

void ShmMmapTableEntry::SetHostMemoryOperationMutex(const std::shared_ptr<std::mutex> &mutex)
{
    if (mutex != nullptr) {
        hostMemoryOperationMutex_ = mutex;
    }
}

void ShmMmapTableEntry::SetClientExitingFlag(const std::shared_ptr<std::atomic<bool>> &clientExiting)
{
    clientExiting_ = clientExiting;
}

bool ShmMmapTableEntry::IsCudaHostMemoryRegistrationDone() const
{
    return pinCompleted_.load(std::memory_order_acquire);
}

bool ShmMmapTableEntry::Contains(const void *pointer) const
{
    const auto address = reinterpret_cast<uintptr_t>(pointer);
    const auto begin = reinterpret_cast<uintptr_t>(pointer_);
    return address >= begin && address - begin < size_;
}

Status ShmMmapTableEntry::GetMemcpySegmentSizes(const void *pointer, size_t size,
                                                std::vector<size_t> &segmentSizes) const
{
    const auto address = reinterpret_cast<uintptr_t>(pointer);
    const auto begin = reinterpret_cast<uintptr_t>(pinRange_.startAddr);
    CHECK_FAIL_RETURN_STATUS(pinRange_.sliceSize > 0, K_RUNTIME_ERROR,
                             "CUDA host-memory pin range is not initialized");
    CHECK_FAIL_RETURN_STATUS(address >= begin && address - begin < pinRange_.totalSize, K_INVALID,
                             "Host pointer is not in this Worker shared memory mapping");
    const size_t offset = static_cast<size_t>(address - begin);
    CHECK_FAIL_RETURN_STATUS(size <= pinRange_.totalSize - offset, K_INVALID,
                             "CUDA memcpy range exceeds the Worker shared memory mapping");
    size_t remaining = size;
    size_t offsetInFragment = offset % pinRange_.sliceSize;
    do {
        const size_t bytes = std::min(remaining, pinRange_.sliceSize - offsetInFragment);
        segmentSizes.emplace_back(bytes);
        remaining -= bytes;
        offsetInFragment = 0;
    } while (remaining > 0);
    return Status::OK();
}

ShmMmapTableEntry::~ShmMmapTableEntry()
{
    try {
        INJECT_POINT_NO_RETURN("ShmMmapTableEntry.Unmap");
    } catch (const std::exception &e) {
        LOG(WARNING) << "Worker shared memory unmap injection failed: " << e.what();
    } catch (...) {
        LOG(WARNING) << "Worker shared memory unmap injection failed with an unknown exception";
    }
    if (pointer_ == nullptr || pointer_ == MAP_FAILED) {
        LOG(ERROR) << FormatString("Mmap pointer is invalid, client id: %s, fd: %d, it may be nullptr", clientId_,
                                   fd_);
        return;
    }
    if (pinAttempted_.load(std::memory_order_acquire)) {
        UnpinHostMemory();
    }
    int ret = munmap(pointer_, size_);
    if (ret != 0) {
        LOG(ERROR) << FormatString("munmap failed, client id: %s, fd: %d, size: %zu, returned: [%d], errno = [%s]",
                                   clientId_, fd_, size_, ret, StrErr(errno));
    } else {
        LOG(INFO) << FormatString("munmap success, client id: %s, fd: %d, size: %zu", clientId_, fd_, size_);
    }
}

void ShmMmapTableEntry::UnpinHostMemory()
{
    std::lock_guard<std::mutex> lock(*hostMemoryOperationMutex_);
    const auto begin = std::chrono::steady_clock::now();
    const size_t totalFragmentCount = GetPinFragmentCount();
    const size_t pinnedFragmentCount = pinnedFragmentCount_.load(std::memory_order_acquire);
    const bool initialClientExiting = clientExiting_ != nullptr && clientExiting_->load(std::memory_order_acquire);
    const bool initialSkipFragmentInterval = initialClientExiting;
    LOG(INFO) << "[CudaHostMemory] Worker shared memory unpin started, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << totalFragmentCount << ", pinnedFragmentCount: " << pinnedFragmentCount
              << ", fragmentIntervalMs: "
              << (initialSkipFragmentInterval ? 0 : HOST_MEMORY_FRAGMENT_INTERVAL.count())
              << ", clientExiting: " << initialClientExiting;
    size_t failedCount = 0;
    for (size_t i = 0; i < pinnedFragmentCount; ++i) {
        const bool clientExiting = clientExiting_ != nullptr && clientExiting_->load(std::memory_order_acquire);
        if (i > 0 && !clientExiting) {
            std::this_thread::sleep_for(HOST_MEMORY_FRAGMENT_INTERVAL);
        }
        if (!UnpinHostMemoryFragment(i)) {
            ++failedCount;
            LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment unpin failed, clientId: " << clientId_
                       << ", fragmentIndex: " << i;
        }
    }
    const auto elapsedUs =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - begin);
    const bool finalClientExiting = clientExiting_ != nullptr && clientExiting_->load(std::memory_order_acquire);
    LOG(INFO) << "[CudaHostMemory] Worker shared memory unpin finished, clientId: " << clientId_
              << ", pointer: " << static_cast<void *>(pointer_) << ", size: " << size_
              << ", fragmentCount: " << totalFragmentCount << ", pinnedFragmentCount: " << pinnedFragmentCount
              << ", attemptedCount: " << pinnedFragmentCount
              << ", successCount: " << pinnedFragmentCount - failedCount << ", failedCount: " << failedCount
              << ", clientExiting: " << finalClientExiting
              << ", completed: true, elapsedUs: " << elapsedUs.count();
}

bool ShmMmapTableEntry::UnpinHostMemoryFragment(size_t fragmentIndex)
{
    const auto fragment = GetPinFragment(fragmentIndex);
    try {
        return UnregisterCudaHostMemory(fragment.pointer);
    } catch (const std::exception &e) {
        LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment unpin failed unexpectedly, clientId: "
                   << clientId_ << ", fragmentIndex: " << fragmentIndex
                   << ", pointer: " << static_cast<void *>(fragment.pointer)
                   << ", size: " << fragment.size << ", error: " << e.what();
    } catch (...) {
        LOG(ERROR) << "[CudaHostMemory] Worker shared memory fragment unpin failed with an unknown exception, "
                   << "clientId: " << clientId_ << ", fragmentIndex: " << fragmentIndex
                   << ", pointer: " << static_cast<void *>(fragment.pointer) << ", size: " << fragment.size;
    }
    return false;
}
}  // namespace client
}  // namespace datasystem
