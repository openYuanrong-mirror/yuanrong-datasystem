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

/** Description: Process-local UB port health monitor. */

#ifndef DATASYSTEM_COMMON_RDMA_UB_PORT_HEALTH_H
#define DATASYSTEM_COMMON_RDMA_UB_PORT_HEALTH_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "datasystem/utils/status.h"

namespace datasystem {

constexpr std::chrono::milliseconds UB_PORT_HEALTH_QUERY_INTERVAL{ 1'000 };

struct UbPortHealthSnapshot {
    uint32_t totalPortCount{ 0 };
    uint32_t badPortCount{ 0 };
};

class IUbPortStatusProvider {
public:
    virtual ~IUbPortStatusProvider() = default;

    virtual Status QueryPortStatus(UbPortHealthSnapshot &snapshot) = 0;
};

class UbPortHealthMonitor {
public:
    // Runs on the monitor thread after a valid query and synchronously during Stop; it must not re-enter this monitor.
    using UpdateCallback = std::function<void(const UbPortHealthSnapshot &)>;

    explicit UbPortHealthMonitor(std::shared_ptr<IUbPortStatusProvider> provider,
                                 std::chrono::milliseconds queryInterval = UB_PORT_HEALTH_QUERY_INTERVAL,
                                 UpdateCallback updateCallback = {});
    ~UbPortHealthMonitor();

    UbPortHealthMonitor(const UbPortHealthMonitor &) = delete;
    UbPortHealthMonitor &operator=(const UbPortHealthMonitor &) = delete;

    Status Start();
    void Stop();
    void TriggerQuery();
    Status CheckAdmission() const;
    std::shared_ptr<const UbPortHealthSnapshot> GetSnapshot() const;

private:
    void Run();
    Status QueryPortStatus(UbPortHealthSnapshot &snapshot) const noexcept;
    Status PublishNoThrow(const UbPortHealthSnapshot &snapshot) noexcept;
    void Publish(const UbPortHealthSnapshot &snapshot);
    void NotifyUpdate(const UbPortHealthSnapshot &snapshot) const noexcept;

    std::shared_ptr<IUbPortStatusProvider> provider_;
    const std::chrono::milliseconds queryInterval_;
    const UpdateCallback updateCallback_;
    mutable std::shared_ptr<const UbPortHealthSnapshot> snapshot_;
    std::atomic<bool> isolated_{ false };
    std::atomic<bool> queryRequested_{ false };

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::thread worker_;
    bool running_{ false };
    bool stopping_{ false };
    bool recoveryTracking_{ false };
    bool hasQueried_{ false };
    std::chrono::steady_clock::time_point nextQueryTime_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_UB_PORT_HEALTH_H
