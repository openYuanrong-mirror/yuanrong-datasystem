/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

/** Description: Client-wide serialized CUDA host-memory pinning/unpinning and shared-memory range lookup. */
#include "datasystem/client/mmap_manager/host_memory_pin_manager.h"

#include <algorithm>
#include <exception>

#include "datasystem/client/mmap_manager/shm_mmap_table_entry.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace client {

HostMemoryPinManager::HostMemoryPinManager() : pinThread_(1, 1, "cuda_host_pin")
{
}

void HostMemoryPinManager::Submit(const std::shared_ptr<ShmMmapTableEntry> &entry)
{
    entry->SetHostMemoryOperationMutex(hostMemoryOperationMutex_);
    entry->SetClientExitingFlag(clientExiting_);
    {
        std::lock_guard<std::mutex> lock(entriesMutex_);
        entries_.erase(std::remove_if(entries_.begin(), entries_.end(),
                                      [](const auto &registeredEntry) { return registeredEntry.expired(); }),
                       entries_.end());
        entries_.emplace_back(entry);
    }
    try {
        pinThread_.Execute([entry] { entry->PinHostMemory(); });
    } catch (const std::exception &e) {
        entry->SkipHostMemoryPin();
        LOG(WARNING) << "Submit CUDA host memory pin task failed: " << e.what();
    } catch (...) {
        entry->SkipHostMemoryPin();
        LOG(WARNING) << "Submit CUDA host memory pin task failed with an unknown exception";
    }
}

void HostMemoryPinManager::MarkClientExiting()
{
    clientExiting_->store(true, std::memory_order_release);
}

Status HostMemoryPinManager::GetMemcpySegmentSizes(const void *hostPointer, size_t size,
                                                   std::vector<size_t> &segmentSizes)
{
    segmentSizes.clear();
    std::shared_ptr<ShmMmapTableEntry> matchedEntry;
    {
        std::lock_guard<std::mutex> lock(entriesMutex_);
        auto output = entries_.begin();
        for (auto iter = entries_.begin(); iter != entries_.end(); ++iter) {
            auto entry = iter->lock();
            if (entry == nullptr) {
                continue;
            }
            *output++ = *iter;
            if (matchedEntry == nullptr && entry->Contains(hostPointer)) {
                matchedEntry = std::move(entry);
            }
        }
        entries_.erase(output, entries_.end());
    }
    if (matchedEntry != nullptr) {
        return matchedEntry->GetMemcpySegmentSizes(hostPointer, size, segmentSizes);
    }
    segmentSizes.emplace_back(size);
    return Status::OK();
}

}  // namespace client
}  // namespace datasystem
