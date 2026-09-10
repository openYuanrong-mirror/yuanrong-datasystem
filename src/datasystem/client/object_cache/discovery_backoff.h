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
 * Description: Exponential backoff gate for background worker-discovery retries.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DISCOVERY_BACKOFF_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DISCOVERY_BACKOFF_H

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>

namespace datasystem {
namespace object_cache {

constexpr int32_t DISCOVERY_BACKOFF_INITIAL_MS = 1'000;
constexpr int32_t DISCOVERY_BACKOFF_MAX_MS = 8'000;
constexpr int32_t DISCOVERY_BACKOFF_GROWTH_FACTOR = 2;

/**
 * @brief Gate background discovery probes after coordinator-reachability failures.
 *
 * Failures widen the gate exponentially (1s, 2s, 4s ... capped at 8s so the worst-case
 * same-node switch-back delay stays bounded); any success reopens it immediately. Time is
 * passed in by the caller so the escalation rhythm is unit-testable without sleeping.
 */
class DiscoveryBackoff {
public:
    ~DiscoveryBackoff() = default;

    bool IsBlocked(std::chrono::steady_clock::time_point now)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return now < blockedUntil_;
    }

    int32_t OnFailure(std::chrono::steady_clock::time_point now)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        blockedUntil_ = now + std::chrono::milliseconds(delayMs_);
        const int32_t appliedMs = delayMs_;
        delayMs_ = std::min(delayMs_ * DISCOVERY_BACKOFF_GROWTH_FACTOR, DISCOVERY_BACKOFF_MAX_MS);
        return appliedMs;
    }

    bool OnSuccess()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const bool wasThrottled = delayMs_ != DISCOVERY_BACKOFF_INITIAL_MS;
        delayMs_ = DISCOVERY_BACKOFF_INITIAL_MS;
        blockedUntil_ = {};
        return wasThrottled;
    }

private:
    std::mutex mutex_;
    std::chrono::steady_clock::time_point blockedUntil_{};
    int32_t delayMs_{ DISCOVERY_BACKOFF_INITIAL_MS };
};
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DISCOVERY_BACKOFF_H
