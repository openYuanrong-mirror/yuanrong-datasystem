/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

/** Description: Client-wide serialized CUDA host-memory pinning/unpinning and shared-memory range lookup. */
#ifndef DATASYSTEM_CLIENT_MMAP_HOST_MEMORY_PIN_MANAGER_H
#define DATASYSTEM_CLIENT_MMAP_HOST_MEMORY_PIN_MANAGER_H

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include "datasystem/client/mmap_manager/shm_mmap_table_entry.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

class HostMemoryPinManager {
public:
    HostMemoryPinManager();
    ~HostMemoryPinManager() = default;

    void Submit(const std::shared_ptr<ShmMmapTableEntry> &entry);

    void MarkClientExiting();

    Status GetMemcpySegmentSizes(const void *hostPointer, size_t size, std::vector<size_t> &segmentSizes);

private:
    ThreadPool pinThread_;
    std::shared_ptr<std::mutex> hostMemoryOperationMutex_{ std::make_shared<std::mutex>() };
    // Shared with mmap entries so delayed Buffer destruction still observes Client shutdown.
    std::shared_ptr<std::atomic<bool>> clientExiting_{ std::make_shared<std::atomic<bool>>(false) };
    std::mutex entriesMutex_;
    std::vector<std::weak_ptr<ShmMmapTableEntry>> entries_;
};

}  // namespace client
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_MMAP_HOST_MEMORY_PIN_MANAGER_H
