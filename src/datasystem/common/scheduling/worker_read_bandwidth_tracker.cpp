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

#include "datasystem/common/scheduling/worker_read_bandwidth_tracker.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <limits>
#include <memory>
#include <thread>
#include <utility>

#include "datasystem/common/flags/common_flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"

namespace datasystem {
namespace scheduling {
namespace {

constexpr uint64_t kNanosecondsPerMicrosecond = 1000ULL;
constexpr uint64_t kNanosecondsPerMillisecond = 1000000ULL;
// Limits the background wait so tracker shutdown normally completes within 10 ms.
constexpr uint64_t kMaximumRefreshPollNs = 10ULL * kNanosecondsPerMillisecond;
constexpr uint64_t kSampleCountMask = 0xFFFFULL;
constexpr uint32_t kSampleCountBits = 16;
constexpr uint64_t maxSampleBits = 256ULL * 1024ULL;

uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs) noexcept
{
    if (rhs != 0 && lhs > std::numeric_limits<uint64_t>::max() / rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

uint64_t PackPercentiles(uint64_t p50Ns, uint64_t p99Ns) noexcept
{
    const uint32_t p50 = static_cast<uint32_t>(std::min<uint64_t>(p50Ns, std::numeric_limits<uint32_t>::max()));
    const uint32_t p99 = static_cast<uint32_t>(std::min<uint64_t>(p99Ns, std::numeric_limits<uint32_t>::max()));
    return (static_cast<uint64_t>(p50) << 32U) | p99;
}

uint64_t SteadyNowNs() noexcept
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

class AtomicByteReleaseGuard {
public:
    explicit AtomicByteReleaseGuard(std::atomic<uint8_t> &flag) noexcept : flag_(flag)
    {
    }

    ~AtomicByteReleaseGuard()
    {
        flag_.store(0, std::memory_order_release);
    }

private:
    std::atomic<uint8_t> &flag_;
};

}  // namespace

WorkerReadBandwidthTrackerConfig WorkerReadBandwidthTrackerConfig::FromFlags()
{
    WorkerReadBandwidthTrackerConfig config;
    config.enabled = FLAGS_load_aware_scheduler_enabled;
    return config;
}

class WorkerReadBandwidthTracker::Impl {
public:
    explicit Impl(WorkerReadBandwidthTrackerConfig config);
    ~Impl() noexcept;

    bool Enabled() const noexcept;
    ReadToken BeginRead(uint32_t observedQueueDepth) const noexcept;
    void CompleteRead(const ReadToken &token, uint64_t dataSizeBytes, bool success);
    Snapshot GetSnapshot() const noexcept;

private:
    struct SampleSlot {
        std::atomic<uint64_t> tag{ 0 };
        std::atomic<uint64_t> latencyNs{ 0 };
        std::atomic<uint8_t> writerBusy{ 0 };
    };

    bool TryAcquireSampleQuota(uint64_t nowNs, uint64_t &samplePeriod) noexcept;
    uint64_t NormalizeLatency(uint64_t latencyNs, uint64_t dataSizeBytes) const noexcept;
    bool StoreLatencySample(uint64_t latencyNs, uint64_t nowNs, uint64_t readId, uint32_t queueDepth);
    bool MarkSampleStoredAndCheckPeriodFull(uint64_t samplePeriod) noexcept;
    bool ReadSample(uint64_t sequence, uint64_t &latencyNs) const noexcept;
    void RequestPercentileRefresh() noexcept;
    void PercentileRefreshLoop() noexcept;
    void RefreshPercentiles(uint64_t nowNs);
    static size_t PercentileIndex(size_t count, uint32_t percentile) noexcept;
    static void CalculateP50P99(uint64_t *values, size_t count, uint64_t &p50Ns, uint64_t &p99Ns) noexcept;
    void LogRead(const ReadToken &token, uint64_t nowNs, uint64_t latencyNs, uint64_t dataSizeBytes) const;
    void LogSample(uint64_t readId, uint64_t nowNs, uint64_t sequence, uint64_t latencyNs, uint32_t queueDepth) const;
    void LogWindow(uint64_t nowNs, uint64_t firstSequence, uint64_t endSequence, size_t count, uint64_t p50Ns,
                   uint64_t p99Ns) const;

    WorkerReadBandwidthTrackerConfig config_;
    uint32_t historyCapacity_;
    uint32_t sampleLimit_;
    uint64_t samplePeriodNs_;
    uint64_t refreshPollIntervalNs_;
    uint64_t baseDataSizeBytes_;
    uint64_t minColDataSizeBytes_;
    std::unique_ptr<SampleSlot[]> samples_;
    std::unique_ptr<uint64_t[]> scratch_;
    std::atomic<uint64_t> packedSampleGate_{ 0 };
    std::atomic<uint64_t> packedStoredGate_{ 0 };
    std::atomic<uint64_t> writeSequence_{ 0 };
    std::atomic<bool> refreshPending_{ false };
    std::atomic<bool> stopRefreshThread_{ false };
    std::thread refreshThread_;
    uint64_t lastPercentileRefreshSequence_{ 0 };
    std::atomic<uint64_t> packedPercentiles_{ 0 };
    std::atomic<uint64_t> latencyVersion_{ 0 };
    mutable std::atomic<uint64_t> readIdCounter_{ 0 };
};

WorkerReadBandwidthTracker::Impl::Impl(WorkerReadBandwidthTrackerConfig config)
    : config_(std::move(config)),
      historyCapacity_(std::max<uint32_t>(1, config_.latencyHistorySamples)),
      sampleLimit_(std::min<uint32_t>(static_cast<uint32_t>(kSampleCountMask),
                                      std::max<uint32_t>(1, config_.latencySamplesPerPeriod))),
      samplePeriodNs_(
          SaturatingMultiply(std::max<uint64_t>(1, config_.latencySamplePeriodMs), kNanosecondsPerMillisecond)),
      refreshPollIntervalNs_(std::min(samplePeriodNs_, kMaximumRefreshPollNs)),
      baseDataSizeBytes_(std::max<uint64_t>(1, config_.latencyBaseDataSizeBytes)),
      minColDataSizeBytes_(maxSampleBits),
      samples_(new SampleSlot[historyCapacity_]),
      scratch_(new uint64_t[historyCapacity_])
{
    const uint64_t initialP50Ns = SaturatingMultiply(config_.initialP50Us, kNanosecondsPerMicrosecond);
    const uint64_t initialP99Ns = SaturatingMultiply(config_.initialP99Us, kNanosecondsPerMicrosecond);
    packedPercentiles_.store(PackPercentiles(initialP50Ns, initialP99Ns), std::memory_order_relaxed);
    if (Enabled()) {
        refreshThread_ = std::thread(&Impl::PercentileRefreshLoop, this);
    }
}

WorkerReadBandwidthTracker::Impl::~Impl() noexcept
{
    stopRefreshThread_.store(true, std::memory_order_release);
    if (refreshThread_.joinable()) {
        refreshThread_.join();
    }
}

bool WorkerReadBandwidthTracker::Impl::Enabled() const noexcept
{
    return config_.enabled;
}

WorkerReadBandwidthTracker::ReadToken WorkerReadBandwidthTracker::Impl::BeginRead(
    uint32_t observedQueueDepth) const noexcept
{
    if (!Enabled()) {
        return {};
    }
    uint64_t readId = 0;
    if (VLOG_IS_ON(1)) {
        readId = readIdCounter_.fetch_add(1, std::memory_order_relaxed) + 1;
    }
    const uint32_t queueDepth = observedQueueDepth == 0 ? 1 : observedQueueDepth;
    return { Clock::now(), readId, queueDepth, true };
}

void WorkerReadBandwidthTracker::Impl::CompleteRead(const ReadToken &token, uint64_t dataSizeBytes, bool success)
{
    if (!Enabled() || !token.valid || !success || dataSizeBytes == 0 || dataSizeBytes < minColDataSizeBytes_) {
        return;
    }
    const auto completedAt = Clock::now();
    const auto latencySigned = std::chrono::duration_cast<std::chrono::nanoseconds>(completedAt - token.start).count();
    if (latencySigned <= 0) {
        return;
    }
    const uint64_t nowNs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(completedAt.time_since_epoch()).count());

    LogRead(token, nowNs, latencySigned, dataSizeBytes);
    uint64_t samplePeriod = 0;
    if (!TryAcquireSampleQuota(nowNs, samplePeriod)) {
        return;
    }
    const uint64_t latencyNs = NormalizeLatency(static_cast<uint64_t>(latencySigned), dataSizeBytes);
    if (latencyNs == 0) {
        return;
    }
    if (!StoreLatencySample(latencyNs, nowNs, token.readId, token.queueDepth)) {
        return;
    }
    if (MarkSampleStoredAndCheckPeriodFull(samplePeriod)) {
        RequestPercentileRefresh();
    }
}

WorkerReadBandwidthTracker::Snapshot WorkerReadBandwidthTracker::Impl::GetSnapshot() const noexcept
{
    if (!Enabled()) {
        return {};
    }
    Snapshot snapshot;
    snapshot.latencyVersion = latencyVersion_.load(std::memory_order_acquire);
    const uint64_t packed = packedPercentiles_.load(std::memory_order_relaxed);
    snapshot.p50Ns = static_cast<uint32_t>(packed >> 32U);
    snapshot.p99Ns = static_cast<uint32_t>(packed & 0xFFFFFFFFULL);
    return snapshot;
}

// Atomically reserve a per-period quota without letting delayed samples roll the period backward.
// samplePeriod is valid only on success; relaxed ordering does not publish sample data.
bool WorkerReadBandwidthTracker::Impl::TryAcquireSampleQuota(uint64_t nowNs, uint64_t &samplePeriod) noexcept
{
    const uint64_t currentPeriod = nowNs / samplePeriodNs_;
    uint64_t packed = packedSampleGate_.load(std::memory_order_relaxed);
    while (true) {
        const uint64_t period = packed >> kSampleCountBits;
        const uint32_t count = static_cast<uint32_t>(packed & kSampleCountMask);
        if (period > currentPeriod) {
            return false;
        }
        if (period == currentPeriod && count >= sampleLimit_) {
            return false;
        }
        const uint32_t nextCount = period == currentPeriod ? count + 1 : 1;
        const uint64_t desired = (currentPeriod << kSampleCountBits) | nextCount;
        if (packedSampleGate_.compare_exchange_weak(packed, desired, std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
            samplePeriod = currentPeriod;
            return true;
        }
    }
}

// @brief Mark one sample as successfully stored and check whether this period reaches full sample limit.
//        Use lock-free CAS to update atomic packed state: high bits = period ID, low bits = stored sample count.
//        Discard delayed samples that belong to an older period.
// @param samplePeriod The period ID of the sample to mark as stored.
// @return true if this stored sample fills up the period (count hits sampleLimit_ after increment);
//         false if update failed, sample is stale, or period still has remaining quota.
// @note Uses std::memory_order_relaxed, no cross-thread memory synchronization.
// @noexcept
bool WorkerReadBandwidthTracker::Impl::MarkSampleStoredAndCheckPeriodFull(uint64_t samplePeriod) noexcept
{
    uint64_t packed = packedStoredGate_.load(std::memory_order_relaxed);

    while (true) {
        const uint64_t storedPeriod = packed >> kSampleCountBits;
        const uint32_t storedCount = static_cast<uint32_t>(packed & kSampleCountMask);

        // Ignore a delayed sample belonging to an older period.
        if (storedPeriod > samplePeriod) {
            return false;
        }

        if (storedPeriod == samplePeriod && storedCount >= sampleLimit_) {
            return false;
        }

        const uint32_t nextCount = storedPeriod == samplePeriod ? storedCount + 1U : 1U;
        const uint64_t desired = (samplePeriod << kSampleCountBits) | nextCount;

        if (packedStoredGate_.compare_exchange_weak(packed, desired, std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
            return nextCount == sampleLimit_;
        }
    }
}

uint64_t WorkerReadBandwidthTracker::Impl::NormalizeLatency(uint64_t latencyNs, uint64_t dataSizeBytes) const noexcept
{
    if (latencyNs == 0 || dataSizeBytes == 0) {
        return 0;
    }
    if (dataSizeBytes == baseDataSizeBytes_) {
        return latencyNs;
    }
#if defined(__SIZEOF_INT128__)
    const __uint128_t product = static_cast<__uint128_t>(latencyNs) * baseDataSizeBytes_;
    const __uint128_t normalized = product / dataSizeBytes;
    if (normalized > std::numeric_limits<uint64_t>::max()) {
        return std::numeric_limits<uint64_t>::max();
    }
    return std::max<uint64_t>(1, static_cast<uint64_t>(normalized));
#else
    if (latencyNs > std::numeric_limits<uint64_t>::max() / baseDataSizeBytes_) {
        return std::numeric_limits<uint64_t>::max();
    }
    return std::max<uint64_t>(1, (latencyNs * baseDataSizeBytes_) / dataSizeBytes);
#endif
}

bool WorkerReadBandwidthTracker::Impl::StoreLatencySample(uint64_t latencyNs, uint64_t nowNs, uint64_t readId,
                                                          uint32_t queueDepth)
{
    const uint64_t sequence = writeSequence_.fetch_add(1, std::memory_order_acq_rel) + 1;
    LogSample(readId, nowNs, sequence, latencyNs, queueDepth);
    SampleSlot &slot = samples_[static_cast<size_t>((sequence - 1) % historyCapacity_)];
    uint8_t expected = 0;
    if (!slot.writerBusy.compare_exchange_strong(expected, 1, std::memory_order_acq_rel, std::memory_order_relaxed)) {
        return false;
    }
    AtomicByteReleaseGuard guard(slot.writerBusy);
    const uint64_t published = slot.tag.load(std::memory_order_acquire);
    if (published >= sequence) {
        return false;
    }
    slot.tag.store(0, std::memory_order_release);
    slot.latencyNs.store(latencyNs, std::memory_order_relaxed);
    slot.tag.store(sequence, std::memory_order_release);
    return true;
}

bool WorkerReadBandwidthTracker::Impl::ReadSample(uint64_t sequence, uint64_t &latencyNs) const noexcept
{
    const SampleSlot &slot = samples_[static_cast<size_t>((sequence - 1) % historyCapacity_)];
    const uint64_t before = slot.tag.load(std::memory_order_acquire);
    if (before != sequence) {
        return false;
    }
    const uint64_t value = slot.latencyNs.load(std::memory_order_relaxed);
    const uint64_t after = slot.tag.load(std::memory_order_acquire);
    if (after != sequence || value == 0) {
        return false;
    }
    latencyNs = value;
    return true;
}

void WorkerReadBandwidthTracker::Impl::RequestPercentileRefresh() noexcept
{
    refreshPending_.store(true, std::memory_order_release);
}

void WorkerReadBandwidthTracker::Impl::PercentileRefreshLoop() noexcept
{
    const auto pollInterval = std::chrono::nanoseconds(refreshPollIntervalNs_);
    while (!stopRefreshThread_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(pollInterval);
        if (stopRefreshThread_.load(std::memory_order_acquire)) {
            break;
        }
        if (refreshPending_.exchange(false, std::memory_order_acq_rel)) {
            RefreshPercentiles(SteadyNowNs());
        }
    }
}

void WorkerReadBandwidthTracker::Impl::RefreshPercentiles(uint64_t nowNs)
{
    const uint64_t endSequence = writeSequence_.load(std::memory_order_acquire);
    if (endSequence == 0 || endSequence == lastPercentileRefreshSequence_) {
        return;
    }
    const uint64_t wanted = std::min<uint64_t>(endSequence, historyCapacity_);
    const uint64_t firstSequence = endSequence - wanted + 1;
    size_t count = 0;
    for (uint64_t sequence = firstSequence; sequence <= endSequence; ++sequence) {
        uint64_t value = 0;
        if (ReadSample(sequence, value)) {
            scratch_[count++] = value;
        }
    }
    if (count == 0) {
        return;
    }
    uint64_t p50Ns = 0;
    uint64_t p99Ns = 0;
    CalculateP50P99(scratch_.get(), count, p50Ns, p99Ns);
    LogWindow(nowNs, firstSequence, endSequence, count, p50Ns, p99Ns);
    packedPercentiles_.store(PackPercentiles(p50Ns, p99Ns), std::memory_order_relaxed);
    latencyVersion_.fetch_add(1, std::memory_order_release);
    lastPercentileRefreshSequence_ = endSequence;
}

size_t WorkerReadBandwidthTracker::Impl::PercentileIndex(size_t count, uint32_t percentile) noexcept
{
    const size_t rank = std::max<size_t>(1, (static_cast<size_t>(percentile) * count + 99U) / 100U);
    return std::min(count - 1, rank - 1);
}

void WorkerReadBandwidthTracker::Impl::CalculateP50P99(uint64_t *values, size_t count, uint64_t &p50Ns,
                                                       uint64_t &p99Ns) noexcept
{
    const size_t p50Index = PercentileIndex(count, 50);
    const size_t p99Index = PercentileIndex(count, 99);
    std::nth_element(values, values + p50Index, values + count);
    p50Ns = values[p50Index];
    if (p99Index == p50Index) {
        p99Ns = p50Ns;
        return;
    }
    std::nth_element(values + p50Index + 1, values + p99Index, values + count);
    p99Ns = values[p99Index];
}

void WorkerReadBandwidthTracker::Impl::LogRead(const ReadToken &token, uint64_t nowNs, uint64_t rawLatencyNs,
                                               uint64_t dataSizeBytes) const
{
    const uint64_t latencyNs = NormalizeLatency(rawLatencyNs, dataSizeBytes);
    VLOG(1) << "[RBS_W] event=READ trace=" << Trace::Instance().GetTraceIDPtr() << " read_id=" << token.readId
            << " ts_ns=" << nowNs << " lat_ns=" << latencyNs << " data_bytes=" << dataSizeBytes
            << " queue_depth=" << token.queueDepth;
}

void WorkerReadBandwidthTracker::Impl::LogSample(uint64_t readId, uint64_t nowNs, uint64_t sequence, uint64_t latencyNs,
                                                 uint32_t queueDepth) const
{
    VLOG(1) << "[RBS_W] event=SAMPLE trace=" << Trace::Instance().GetTraceIDPtr() << " read_id=" << readId
            << " ts_ns=" << nowNs << " seq=" << sequence << " lat_ns=" << latencyNs << " queue_depth=" << queueDepth;
}

void WorkerReadBandwidthTracker::Impl::LogWindow(uint64_t nowNs, uint64_t firstSequence, uint64_t endSequence,
                                                 size_t count, uint64_t p50Ns, uint64_t p99Ns) const
{
    VLOG(1) << "[RBS_W] event=WINDOW_END trace=" << Trace::Instance().GetTraceIDPtr() << " ts_ns=" << nowNs
            << " seq_first=" << firstSequence << " seq_end=" << endSequence << " sample_count=" << count
            << " p50_ns=" << p50Ns << " p99_ns=" << p99Ns;
}

WorkerReadBandwidthTracker &WorkerReadBandwidthTracker::Instance()
{
    static WorkerReadBandwidthTracker instance(WorkerReadBandwidthTrackerConfig::FromFlags());
    return instance;
}

WorkerReadBandwidthTracker::WorkerReadBandwidthTracker(WorkerReadBandwidthTrackerConfig config)
    : impl_(new Impl(std::move(config)))
{
}

WorkerReadBandwidthTracker::~WorkerReadBandwidthTracker() = default;

bool WorkerReadBandwidthTracker::Enabled() const noexcept
{
    return impl_->Enabled();
}

WorkerReadBandwidthTracker::ReadToken WorkerReadBandwidthTracker::BeginRead(uint32_t observedQueueDepth) const noexcept
{
    return impl_->BeginRead(observedQueueDepth);
}

void WorkerReadBandwidthTracker::CompleteRead(const ReadToken &token, uint64_t dataSizeBytes, bool success)
{
    impl_->CompleteRead(token, dataSizeBytes, success);
}

WorkerReadBandwidthTracker::Snapshot WorkerReadBandwidthTracker::GetSnapshot() const noexcept
{
    return impl_->GetSnapshot();
}

}  // namespace scheduling
}  // namespace datasystem
