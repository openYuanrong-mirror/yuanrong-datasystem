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

/** Description: Worker-side read latency sampling and P50/P99 tracking. */
#ifndef DATASYSTEM_COMMON_SCHEDULING_WORKER_READ_BANDWIDTH_TRACKER_H
#define DATASYSTEM_COMMON_SCHEDULING_WORKER_READ_BANDWIDTH_TRACKER_H

#include <chrono>
#include <cstdint>
#include <memory>

namespace datasystem {
namespace ut {
class WorkerReadBandwidthTrackerTest;
}

namespace scheduling {

struct WorkerReadBandwidthTrackerConfig {
    bool enabled{ false };
    uint64_t latencySamplePeriodMs{ 1 };
    uint32_t latencySamplesPerPeriod{ 4 };
    uint32_t latencyHistorySamples{ 1000 };
    uint64_t latencyBaseDataSizeBytes{ 4ULL * 1024ULL * 1024ULL };
    uint64_t initialP50Us{ 200 };
    uint64_t initialP99Us{ 500 };

    static WorkerReadBandwidthTrackerConfig FromFlags();
};

class WorkerReadBandwidthTracker {
public:
    using Clock = std::chrono::steady_clock;

    // ReadToken is immutable. The caller must complete each token at most once.
    struct ReadToken {
        Clock::time_point start{};
        uint64_t readId{ 0 };
        uint32_t queueDepth{ 1 };
        bool valid{ false };
    };

    struct Snapshot {
        uint32_t p50Ns{ 0 };
        uint32_t p99Ns{ 0 };
        uint64_t latencyVersion{ 0 };
    };

    static WorkerReadBandwidthTracker &Instance();

    ~WorkerReadBandwidthTracker();

    WorkerReadBandwidthTracker(const WorkerReadBandwidthTracker &) = delete;
    WorkerReadBandwidthTracker &operator=(const WorkerReadBandwidthTracker &) = delete;

    bool Enabled() const noexcept;
    ReadToken BeginRead(uint32_t observedQueueDepth = 0) const noexcept;

    // Failed or zero-sized reads are intentionally ignored without side effects.
    void CompleteRead(const ReadToken &token, uint64_t dataSizeBytes, bool success);

    Snapshot GetSnapshot() const noexcept;

private:
    friend class ut::WorkerReadBandwidthTrackerTest;

    explicit WorkerReadBandwidthTracker(WorkerReadBandwidthTrackerConfig config);

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace scheduling
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SCHEDULING_WORKER_READ_BANDWIDTH_TRACKER_H
