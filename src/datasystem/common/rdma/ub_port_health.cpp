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

#include "datasystem/common/rdma/ub_port_health.h"

#include <exception>
#include <string>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

constexpr int UB_PORT_HEALTH_LOG_EVERY_N = 30;

UbPortHealthMonitor::UbPortHealthMonitor(std::shared_ptr<IUbPortStatusProvider> provider,
                                         std::chrono::milliseconds queryInterval, UpdateCallback updateCallback)
    : provider_(std::move(provider)), queryInterval_(queryInterval), updateCallback_(std::move(updateCallback))
{
}

UbPortHealthMonitor::~UbPortHealthMonitor()
{
    Stop();
}

Status UbPortHealthMonitor::Start()
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(!stopping_, K_NOT_READY, "UB port health monitor is stopping");
    if (running_) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(provider_ != nullptr, K_INVALID, "UB port status provider is null");
    CHECK_FAIL_RETURN_STATUS(queryInterval_ > std::chrono::milliseconds::zero(), K_INVALID,
                             "UB port health query interval must be positive");
    stopping_ = false;
    try {
        worker_ = std::thread(&UbPortHealthMonitor::Run, this);
    } catch (const std::exception &e) {
        return Status(K_RUNTIME_ERROR, std::string("Failed to start UB port health monitor: ") + e.what());
    }
    running_ = true;
    return Status::OK();
}

void UbPortHealthMonitor::Stop()
{
    std::thread worker;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (stopping_) {
            cv_.wait(lock, [this] { return !stopping_; });
            return;
        }
        stopping_ = true;
        if (running_) {
            cv_.notify_all();
            worker = std::move(worker_);
        }
    }
    if (worker.joinable()) {
        worker.join();
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        running_ = false;
        recoveryTracking_ = false;
        hasQueried_ = false;
        nextQueryTime_ = {};
    }
    queryRequested_.store(false, std::memory_order_release);
    isolated_.store(false, std::memory_order_release);
    std::atomic_store_explicit(&snapshot_, std::shared_ptr<const UbPortHealthSnapshot>{}, std::memory_order_release);
    NotifyUpdate({});
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = false;
        cv_.notify_all();
    }
}

void UbPortHealthMonitor::TriggerQuery()
{
    if (queryRequested_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_ || stopping_) {
        queryRequested_.store(false, std::memory_order_release);
        return;
    }
    cv_.notify_one();
}

Status UbPortHealthMonitor::CheckAdmission() const
{
    if (!isolated_.load(std::memory_order_acquire)) {
        return Status::OK();
    }
    return Status(K_URMA_WORKER_UNAVAILABLE, "Client-local UB endpoint unavailable: all ports are BAD");
}

std::shared_ptr<const UbPortHealthSnapshot> UbPortHealthMonitor::GetSnapshot() const
{
    return std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
}

Status UbPortHealthMonitor::QueryPortStatus(UbPortHealthSnapshot &snapshot) const noexcept
{
    Status rc;
    try {
        rc = provider_->QueryPortStatus(snapshot);
    } catch (const std::exception &e) {
        rc = Status(K_RUNTIME_ERROR, std::string("UB port status provider threw: ") + e.what());
    } catch (...) {
        rc = Status(K_RUNTIME_ERROR, "UB port status provider threw an unknown exception");
    }
    if (rc.IsOk() && (snapshot.totalPortCount == 0 || snapshot.badPortCount > snapshot.totalPortCount)) {
        rc = Status(K_INVALID, "UB port status result has invalid port counts");
    }
    return rc;
}

Status UbPortHealthMonitor::PublishNoThrow(const UbPortHealthSnapshot &snapshot) noexcept
{
    try {
        Publish(snapshot);
    } catch (const std::exception &e) {
        return Status(K_RUNTIME_ERROR, std::string("Failed to publish client-local UB port health: ") + e.what());
    } catch (...) {
        return Status(K_RUNTIME_ERROR, "Failed to publish client-local UB port health: unknown exception");
    }
    return Status::OK();
}

void UbPortHealthMonitor::Run()
{
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stopping_) {
        if (!queryRequested_.load(std::memory_order_acquire) && !recoveryTracking_) {
            cv_.wait(lock, [this] {
                return stopping_ || queryRequested_.load(std::memory_order_acquire) || recoveryTracking_;
            });
            continue;
        }
        if (hasQueried_ && std::chrono::steady_clock::now() < nextQueryTime_) {
            cv_.wait_until(lock, nextQueryTime_, [this] { return stopping_; });
            continue;
        }

        queryRequested_.store(false, std::memory_order_release);
        const bool wasTrackingRecovery = recoveryTracking_;
        lock.unlock();
        UbPortHealthSnapshot candidate;
        Status rc = QueryPortStatus(candidate);
        lock.lock();

        if (stopping_) {
            continue;
        }

        hasQueried_ = true;
        nextQueryTime_ = std::chrono::steady_clock::now() + queryInterval_;
        if (rc.IsError()) {
            recoveryTracking_ = wasTrackingRecovery;
            LOG_FIRST_AND_EVERY_N(WARNING, UB_PORT_HEALTH_LOG_EVERY_N)
                << "Failed to query client-local UB port health: " << rc;
            continue;
        }

        const bool allBad = candidate.totalPortCount != 0
                            && candidate.badPortCount == candidate.totalPortCount;
        recoveryTracking_ = allBad || (wasTrackingRecovery && candidate.badPortCount != 0);
        lock.unlock();
        rc = PublishNoThrow(candidate);
        lock.lock();
        if (rc.IsError()) {
            recoveryTracking_ = true;
            LOG_FIRST_AND_EVERY_N(ERROR, UB_PORT_HEALTH_LOG_EVERY_N) << rc;
            continue;
        }
    }
}

void UbPortHealthMonitor::Publish(const UbPortHealthSnapshot &snapshot)
{
    auto next = std::make_shared<const UbPortHealthSnapshot>(snapshot);
    const bool allBad = snapshot.totalPortCount != 0 && snapshot.badPortCount == snapshot.totalPortCount;
    const bool wasIsolated = isolated_.load(std::memory_order_acquire);
    if (allBad) {
        std::atomic_store_explicit(&snapshot_, std::move(next), std::memory_order_release);
        isolated_.store(true, std::memory_order_release);
    } else {
        isolated_.store(false, std::memory_order_release);
        std::atomic_store_explicit(&snapshot_, std::move(next), std::memory_order_release);
    }
    if (wasIsolated != allBad) {
        LOG(WARNING) << "Client-local UB isolation changed, isolated=" << allBad
                     << ", badPorts=" << snapshot.badPortCount << ", totalPorts=" << snapshot.totalPortCount;
    }
    NotifyUpdate(snapshot);
}

void UbPortHealthMonitor::NotifyUpdate(const UbPortHealthSnapshot &snapshot) const noexcept
{
    if (!updateCallback_) {
        return;
    }
    try {
        updateCallback_(snapshot);
    } catch (const std::exception &e) {
        LOG(ERROR) << "UB port health update callback threw: " << e.what();
    } catch (...) {
        LOG(ERROR) << "UB port health update callback threw an unknown exception";
    }
}

}  // namespace datasystem
