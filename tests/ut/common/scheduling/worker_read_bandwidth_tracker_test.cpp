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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/scheduling/worker_read_bandwidth_tracker.h"

namespace datasystem {
namespace ut {
namespace {
using scheduling::WorkerReadBandwidthTrackerConfig;
using scheduling::WorkerReadBandwidthTracker;
using Clock = WorkerReadBandwidthTracker::Clock;

static_assert(!std::is_constructible_v<WorkerReadBandwidthTracker, WorkerReadBandwidthTrackerConfig>,
              "Production callers must use WorkerReadBandwidthTracker::Instance()");

constexpr uint64_t kBaseBytes = 4ULL * 1024 * 1024;
constexpr uint64_t kMinimumBytes = 256ULL * 1024;
constexpr uint64_t kNsPerMs = 1000000;
constexpr uint32_t kHistorySize = 8;
constexpr auto kWaitTimeout = std::chrono::seconds(2);
constexpr auto kPollInterval = std::chrono::milliseconds(1);

WorkerReadBandwidthTrackerConfig WorkerConfig()
{
    WorkerReadBandwidthTrackerConfig config;
    config.enabled = true;
    config.latencySamplePeriodMs = 1;
    config.latencySamplesPerPeriod = 1;
    config.latencyHistorySamples = kHistorySize;
    config.latencyBaseDataSizeBytes = kBaseBytes;
    return config;
}

bool WaitForVersion(WorkerReadBandwidthTracker &tracker, uint64_t previous)
{
    const auto deadline = Clock::now() + kWaitTimeout;
    do {
        if (tracker.GetSnapshot().latencyVersion > previous) {
            return true;
        }
        std::this_thread::sleep_for(kPollInterval);
    } while (Clock::now() < deadline);
    return false;
}

struct LatencyBounds {
    uint32_t lower;
    uint32_t upper;
};

LatencyBounds CompleteSample(WorkerReadBandwidthTracker &tracker, uint64_t bytes = kBaseBytes,
                             uint32_t latencyMs = 100)
{
    auto token = tracker.BeginRead();
    token.start = Clock::now() - std::chrono::milliseconds(latencyMs);
    const auto before = Clock::now();
    tracker.CompleteRead(token, bytes, true);
    const auto after = Clock::now();
    const auto normalize = [&](Clock::time_point time) {
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(time - token.start).count();
        return static_cast<uint32_t>(std::min<uint64_t>(
            static_cast<uint64_t>(ns) * kBaseBytes / bytes, std::numeric_limits<uint32_t>::max()));
    };
    return { normalize(before), normalize(after) };
}

void ExpectWithin(uint32_t value, const LatencyBounds &bounds)
{
    EXPECT_GE(value, bounds.lower);
    EXPECT_LE(value, bounds.upper);
}
}  // namespace

// The tracker's constructor is protected (production callers must use Instance());
// tests reach it through a subclass wrapper.
class TrackerHandle : public WorkerReadBandwidthTracker {
public:
    explicit TrackerHandle(WorkerReadBandwidthTrackerConfig config) : WorkerReadBandwidthTracker(std::move(config))
    {
    }
};

class WorkerReadBandwidthTrackerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        tracker_.reset(new TrackerHandle(WorkerConfig()));
    }

    static TrackerHandle CreateTracker(WorkerReadBandwidthTrackerConfig config)
    {
        return TrackerHandle(std::move(config));
    }

    void CheckSingleSample(uint64_t bytes)
    {
        const auto bounds = CompleteSample(*tracker_, bytes);
        ASSERT_TRUE(WaitForVersion(*tracker_, 0));
        const auto snapshot = tracker_->GetSnapshot();
        ExpectWithin(snapshot.p50Ns, bounds);
        EXPECT_EQ(snapshot.p50Ns, snapshot.p99Ns);
    }

    void CheckIgnoredRead(uint64_t bytes, bool success, bool valid = true)
    {
        auto token = tracker_->BeginRead();
        token.start = Clock::now() - std::chrono::seconds(1);
        token.valid = valid;
        tracker_->CompleteRead(token, bytes, success);
        // The valid control uses a fresh quota period and must be the only stored sample.
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        const auto bounds = CompleteSample(*tracker_);
        ASSERT_TRUE(WaitForVersion(*tracker_, 0));
        const auto snapshot = tracker_->GetSnapshot();
        ExpectWithin(snapshot.p50Ns, bounds);
        ExpectWithin(snapshot.p99Ns, bounds);
    }

    std::unique_ptr<WorkerReadBandwidthTracker> tracker_;
};

TEST_F(WorkerReadBandwidthTrackerTest, EnabledAndQueueDepth)
{
    EXPECT_TRUE(tracker_->Enabled());
    const auto defaultToken = tracker_->BeginRead();
    EXPECT_TRUE(defaultToken.valid);
    EXPECT_EQ(defaultToken.queueDepth, 1U);
    EXPECT_EQ(tracker_->BeginRead(0).queueDepth, 1U);
    EXPECT_EQ(tracker_->BeginRead(5).queueDepth, 5U);
}

TEST_F(WorkerReadBandwidthTrackerTest, DisabledIgnoresEvenValidToken)
{
    auto config = WorkerConfig();
    config.enabled = false;
    auto disabled = CreateTracker(config);
    EXPECT_FALSE(disabled.Enabled());
    EXPECT_FALSE(disabled.BeginRead().valid);
    disabled.CompleteRead(tracker_->BeginRead(), kBaseBytes, true);
    const auto snapshot = disabled.GetSnapshot();
    EXPECT_EQ(snapshot.latencyVersion, 0U);
    EXPECT_EQ(snapshot.p50Ns, 0U);
    EXPECT_EQ(snapshot.p99Ns, 0U);
}

TEST_F(WorkerReadBandwidthTrackerTest, SnapshotStartsWithConfiguredPercentiles)
{
    auto config = WorkerConfig();
    config.initialP50Us = 123;
    config.initialP99Us = 456;
    auto tracker = CreateTracker(config);
    const auto snapshot = tracker.GetSnapshot();
    EXPECT_EQ(snapshot.latencyVersion, 0U);
    EXPECT_EQ(snapshot.p50Ns, config.initialP50Us * 1000);
    EXPECT_EQ(snapshot.p99Ns, config.initialP99Us * 1000);
}

TEST_F(WorkerReadBandwidthTrackerTest, CompleteReadWithValidDataProducesSnapshot)
{
    CheckSingleSample(kBaseBytes);
}

TEST_F(WorkerReadBandwidthTrackerTest, NormalizeLatencySmallerDataIncreasesLatency)
{
    CheckSingleSample(kBaseBytes / 2);
}

TEST_F(WorkerReadBandwidthTrackerTest, NormalizeLatencyLargerDataDecreasesLatency)
{
    CheckSingleSample(kBaseBytes * 2);
}

TEST_F(WorkerReadBandwidthTrackerTest, MinimumSizeIsIncluded)
{
    CheckSingleSample(kMinimumBytes);
}

TEST_F(WorkerReadBandwidthTrackerTest, FailedReadIsIgnored)
{
    CheckIgnoredRead(kBaseBytes, false);
}

TEST_F(WorkerReadBandwidthTrackerTest, InvalidTokenIsIgnored)
{
    CheckIgnoredRead(kBaseBytes, true, false);
}

TEST_F(WorkerReadBandwidthTrackerTest, ZeroSizeReadIsIgnored)
{
    CheckIgnoredRead(0, true);
}

TEST_F(WorkerReadBandwidthTrackerTest, BelowMinimumSizeIsIgnored)
{
    CheckIgnoredRead(kMinimumBytes - 1, true);
}

TEST_F(WorkerReadBandwidthTrackerTest, PercentilesAndHistoryEviction)
{
    std::vector<LatencyBounds> history;
    uint64_t version = 0;
    for (uint32_t i = 0; i < 2 * kHistorySize; ++i) {
        SCOPED_TRACE(i);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        history.push_back(CompleteSample(*tracker_, kBaseBytes, (i + 1) * 10));
        ASSERT_TRUE(WaitForVersion(*tracker_, version));
        const auto snapshot = tracker_->GetSnapshot();
        EXPECT_GT(snapshot.latencyVersion, version);
        version = snapshot.latencyVersion;
        if (history.size() > kHistorySize) {
            history.erase(history.begin());
        }
        std::vector<uint32_t> lower;
        std::vector<uint32_t> upper;
        for (const auto &bounds : history) {
            lower.push_back(bounds.lower);
            upper.push_back(bounds.upper);
        }
        std::sort(lower.begin(), lower.end());
        std::sort(upper.begin(), upper.end());
        // Nearest-rank P50; P99 is the maximum for these windows of at most eight samples.
        const size_t median = (history.size() - 1) / 2;
        ExpectWithin(snapshot.p50Ns, { lower[median], upper[median] });
        ExpectWithin(snapshot.p99Ns, { lower.back(), upper.back() });
    }
}

TEST_F(WorkerReadBandwidthTrackerTest, SampleQuotaRejectsBurstAndResetsInNextPeriod)
{
    auto config = WorkerConfig();
    config.latencySamplePeriodMs = 100;
    config.latencySamplesPerPeriod = 4;
    const auto period = [&] {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now().time_since_epoch()).count()) / config.latencySamplePeriodMs;
    };
    constexpr uint32_t kBoundaryRetries = 5;
    for (uint32_t retry = 0; retry < kBoundaryRetries; ++retry) {
        auto tracker = CreateTracker(config);
        const auto firstPeriod = period();
        std::vector<LatencyBounds> accepted;
        for (uint32_t i = 0; i < config.latencySamplesPerPeriod; ++i) {
            accepted.push_back(CompleteSample(tracker));
        }
        constexpr uint32_t kBurstSize = 32;
        for (uint32_t i = 0; i < kBurstSize; ++i) {
            CompleteSample(tracker, kBaseBytes, 1000);
        }
        // A preempted producer may cross a quota boundary; retry with an empty tracker.
        if (period() != firstPeriod) {
            continue;
        }
        ASSERT_TRUE(WaitForVersion(tracker, 0));
        const auto first = tracker.GetSnapshot();
        uint32_t upper = 0;
        for (const auto &bounds : accepted) {
            upper = std::max(upper, bounds.upper);
        }
        EXPECT_GE(first.p50Ns, 100 * kNsPerMs);
        EXPECT_LE(first.p99Ns, upper);

        const auto deadline = Clock::now() + kWaitTimeout;
        while (period() == firstPeriod && Clock::now() < deadline) {
            std::this_thread::sleep_for(kPollInterval);
        }
        ASSERT_NE(period(), firstPeriod);
        const auto secondPeriod = period();
        for (uint32_t i = 0; i < config.latencySamplesPerPeriod; ++i) {
            CompleteSample(tracker, kBaseBytes, 1000);
        }
        if (period() != secondPeriod) {
            continue;
        }
        ASSERT_TRUE(WaitForVersion(tracker, first.latencyVersion));
        const auto second = tracker.GetSnapshot();
        EXPECT_GE(second.p99Ns, 1000 * kNsPerMs);
        EXPECT_LE(second.p50Ns, upper);
        return;
    }
    FAIL() << "Could not submit a complete batch within one sample period";
}

TEST_F(WorkerReadBandwidthTrackerTest, ConcurrentWritesAndReadsAcrossHistoryWrap)
{
    auto config = WorkerConfig();
    config.latencyHistorySamples = 2;
    auto tracker = CreateTracker(config);
    std::atomic<bool> start{ false };
    std::atomic<bool> stop{ false };
    std::vector<std::thread> writers;
    constexpr uint32_t kWriterCount = 4;
    for (uint32_t i = 0; i < kWriterCount; ++i) {
        writers.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (!stop.load(std::memory_order_acquire)) {
                CompleteSample(tracker);
                std::this_thread::sleep_for(kPollInterval);
            }
        });
    }
    start.store(true, std::memory_order_release);
    uint64_t lastVersion = 0;
    uint32_t observedUpdates = 0;
    constexpr uint32_t kRequiredUpdates = 16;
    const auto deadline = Clock::now() + kWaitTimeout;
    while (Clock::now() < deadline && observedUpdates < kRequiredUpdates) {
        const auto snapshot = tracker.GetSnapshot();
        EXPECT_GE(snapshot.latencyVersion, lastVersion);
        if (snapshot.latencyVersion != 0) {
            EXPECT_GE(snapshot.p50Ns, 100 * kNsPerMs);
            EXPECT_LE(snapshot.p50Ns, snapshot.p99Ns);
        }
        if (snapshot.latencyVersion > lastVersion) {
            ++observedUpdates;
        }
        lastVersion = snapshot.latencyVersion;
        std::this_thread::yield();
    }
    stop.store(true, std::memory_order_release);
    for (auto &writer : writers) {
        writer.join();
    }
    EXPECT_EQ(observedUpdates, kRequiredUpdates);
}

}  // namespace ut
}  // namespace datasystem
