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

#include "datasystem/client/object_cache/routing/worker_ub_health_registry.h"

#include <atomic>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"

namespace datasystem::client {
namespace {
bool SameRoutingWorkers(const std::unordered_map<HostPort, WorkerUbPortHealth> &lhs,
                        const std::unordered_map<HostPort, WorkerUbPortHealth> &rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (const auto &[worker, health] : lhs) {
        auto other = rhs.find(worker);
        if (other == rhs.end() || health.incarnation != other->second.incarnation
            || !IsSameUbPortHealth(health.portHealth, other->second.portHealth)) {
            return false;
        }
    }
    return true;
}

bool IsRoutableMember(::datasystem::MembershipPb::StatePb state)
{
    return state == ::datasystem::MembershipPb::ACTIVE || state == ::datasystem::MembershipPb::LEAVING;
}

WorkerUbPortHealth BuildRoutingWorker(const std::string &incarnation, const UbHealthSummary &summary)
{
    return { incarnation, *summary.portHealth };
}

std::optional<bool> GetVerifiedUnavailable(const UbHealthSummary &summary, bool verified)
{
    if (!verified) {
        return std::nullopt;
    }
    if (!summary.portHealth.has_value()) {
        return !summary.writable;
    }
    if (CanUpdateRemoteUbAdmission(UbPortHealthEvidenceSource::QUERY_RESPONSE, *summary.portHealth)) {
        return ShouldIsolateForUbPortHealth(*summary.portHealth);
    }
    return std::nullopt;
}
}  // namespace

struct WorkerUbHealthRegistry::WorkerState {
    UbHealthSummaryCache::Snapshot health;
    std::unordered_map<HostPort, std::string> verifiedUnavailable;
};

struct WorkerUbHealthRegistry::State {
    using Incarnations = std::unordered_map<HostPort, std::string>;
    std::shared_ptr<const Incarnations> incarnations;
    std::shared_ptr<const WorkerState> workers = std::make_shared<const WorkerState>();
    UbRoutingHealthSnapshot routing;
};

WorkerUbHealthRegistry::WorkerUbHealthRegistry() : state_(std::make_shared<const State>())
{
}

void WorkerUbHealthRegistry::ReconcileTopology(const ::datasystem::ClusterTopologyPb &topology)
{
    std::lock_guard<std::mutex> lock(writeMutex_);
    auto current = std::atomic_load(&state_);
    auto incarnations = std::make_shared<State::Incarnations>();
    incarnations->reserve(topology.members_size());
    std::unordered_set<HostPort> workers;
    workers.reserve(topology.members_size());
    std::vector<std::pair<HostPort, const ::datasystem::MembershipPb *>> routableMembers;
    routableMembers.reserve(topology.members_size());
    for (const auto &[endpoint, member] : topology.members()) {
        HostPort worker;
        if (!IsRoutableMember(member.state()) || member.id().empty()
            || worker.ParseString(endpoint).IsError()) {
            continue;
        }
        incarnations->emplace(worker, member.id());
        workers.emplace(worker);
        routableMembers.emplace_back(worker, &member);
    }
    if (current->incarnations != nullptr && *current->incarnations == *incarnations) {
        return;
    }

    auto next = std::make_shared<State>();
    auto workerState = std::make_shared<WorkerState>();
    workerState->health = current->workers->health;
    next->routing.localClient = current->routing.localClient;
    next->routing.workers.reserve(routableMembers.size());
    for (const auto &[worker, member] : routableMembers) {
        ReconcileTopologyMemberLocked(current, worker, *member, *next, *workerState);
    }
    workerState->health.ReconcileWorkers(workers);
    if (current->workers->health.Size() == workerState->health.Size()
        && current->workers->verifiedUnavailable == workerState->verifiedUnavailable
        && SameRoutingWorkers(current->routing.workers, next->routing.workers)) {
        return;
    }
    next->incarnations = std::move(incarnations);
    next->workers = std::move(workerState);
    std::atomic_store(&state_, std::shared_ptr<const State>(std::move(next)));
}

void WorkerUbHealthRegistry::ReconcileTopologyMemberLocked(
    const std::shared_ptr<const State> &current, const HostPort &worker,
    const ::datasystem::MembershipPb &member, State &next, WorkerState &workers) const
{
    if (current->incarnations != nullptr) {
        auto previous = current->incarnations->find(worker);
        if (previous != current->incarnations->end() && previous->second != member.id()) {
            workers.health.Retire(worker, previous->second);
        }
    }
    const auto *summary = workers.health.Find(worker);
    if (summary != nullptr && summary->incarnation != member.id()) {
        workers.health.Retire(worker, summary->incarnation);
        summary = nullptr;
    }
    if (summary != nullptr && summary->portHealth.has_value()) {
        next.routing.workers.emplace(worker, BuildRoutingWorker(member.id(), *summary));
    }
    auto verified = current->workers->verifiedUnavailable.find(worker);
    if (verified != current->workers->verifiedUnavailable.end() && verified->second == member.id()) {
        workers.verifiedUnavailable.emplace(worker, verified->second);
    }
}

bool WorkerUbHealthRegistry::ApplySummary(const UbHealthSummary &summary, const std::string &expectedIncarnation)
{
    return ApplySummaryInternal(summary, expectedIncarnation, false) == ApplyResult::UPDATED;
}

bool WorkerUbHealthRegistry::ApplyVerifiedSummary(const UbHealthSummary &summary,
                                                  const std::string &expectedIncarnation)
{
    return ApplySummaryInternal(summary, expectedIncarnation, true) != ApplyResult::REJECTED;
}

WorkerUbHealthRegistry::ApplyResult WorkerUbHealthRegistry::ApplySummaryInternal(
    const UbHealthSummary &summary, const std::string &expectedIncarnation, bool verified)
{
    std::unique_lock<std::mutex> lock(writeMutex_);
    auto current = std::atomic_load(&state_);
    std::string expected;
    if (!ResolveExpectedIncarnationLocked(*current, summary.worker, expectedIncarnation, expected)) {
        return ApplyResult::REJECTED;
    }
    UbHealthSummary accepted;
    if (!current->workers->health.Prepare(summary, expected, accepted)) {
        return ApplyResult::REJECTED;
    }
    const bool evidenceAccepted = !verified || !summary.portHealth.has_value()
                                  || IsSameUbPortHealth(summary.portHealth, accepted.portHealth);
    const auto *previous = current->workers->health.Find(summary.worker);
    const bool summaryChanged = previous == nullptr || !IsSameUbHealthSummary(*previous, accepted);
    const auto unavailable = GetVerifiedUnavailable(accepted, verified && evidenceAccepted);
    auto marked = current->workers->verifiedUnavailable.find(summary.worker);
    const bool identityChanged = marked != current->workers->verifiedUnavailable.end()
                                 && marked->second != summary.incarnation;
    const bool currentlyVerified = marked != current->workers->verifiedUnavailable.end() && !identityChanged;
    if (!summaryChanged && !identityChanged
        && (!unavailable.has_value() || *unavailable == currentlyVerified)) {
        return verified && evidenceAccepted ? ApplyResult::ACCEPTED : ApplyResult::REJECTED;
    }
    auto next = std::make_shared<State>(*current);
    auto workers = std::make_shared<WorkerState>(*current->workers);
    workers->health.Apply(summary, expected);
    if (summaryChanged && accepted.portHealth.has_value()) {
        next->routing.workers[summary.worker] = BuildRoutingWorker(expected, accepted);
    }
    if (identityChanged || (unavailable.has_value() && !*unavailable)) {
        workers->verifiedUnavailable.erase(summary.worker);
    }
    if (unavailable.value_or(false)) {
        workers->verifiedUnavailable[summary.worker] = summary.incarnation;
    }
    next->workers = std::move(workers);
    std::atomic_store(&state_, std::shared_ptr<const State>(std::move(next)));
    const bool admissionChanged = unavailable.has_value() && *unavailable != currentlyVerified;
    lock.unlock();
    if (admissionChanged) {
        LogAdmissionChange(summary, accepted, *unavailable);
    }
    return evidenceAccepted ? ApplyResult::UPDATED : ApplyResult::REJECTED;
}

bool WorkerUbHealthRegistry::ResolveExpectedIncarnationLocked(const State &current, const HostPort &worker,
                                                              const std::string &fallback,
                                                              std::string &expected) const
{
    expected = fallback;
    if (current.incarnations == nullptr) {
        return true;
    }
    auto identity = current.incarnations->find(worker);
    if (identity == current.incarnations->end()) {
        return false;
    }
    expected = identity->second;
    return true;
}

void WorkerUbHealthRegistry::LogAdmissionChange(const UbHealthSummary &summary, const UbHealthSummary &accepted,
                                                bool unavailable) const
{
    if (accepted.portHealth.has_value()) {
        LOG(INFO) << "UB_PORT_HEALTH action=" << (unavailable ? "isolate" : "recover")
                  << " peer=" << summary.worker.ToString() << " incarnation=" << summary.incarnation
                  << " source=query_response health_epoch=" << accepted.portHealth->healthEpoch
                  << " bad=" << accepted.portHealth->badPortCount
                  << " total=" << accepted.portHealth->totalPortCount;
        return;
    }
    LOG(INFO) << "UB_PORT_HEALTH action=" << (unavailable ? "isolate" : "recover")
              << " peer=" << summary.worker.ToString() << " incarnation=" << summary.incarnation
              << " source=query_response legacy_summary=true";
}

bool WorkerUbHealthRegistry::ApplyLocalClientPortHealth(const UbPortHealthSummary &portHealth)
{
    // Keep the retired snapshot alive until the publication lock has been released.
    std::shared_ptr<const State> current;
    std::lock_guard<std::mutex> lock(writeMutex_);
    current = std::atomic_load(&state_);
    std::optional<UbPortHealthSummary> merged;
    if (!MergeUbPortHealth(current->routing.localClient, portHealth, merged)
        || !merged.has_value() || IsSameUbPortHealth(current->routing.localClient, *merged)) {
        return false;
    }
    auto next = std::make_shared<State>(*current);
    next->routing.localClient = *merged;
    std::atomic_store(&state_, std::shared_ptr<const State>(std::move(next)));
    return true;
}

void WorkerUbHealthRegistry::OnUbPortHealthChanged(const UbPortHealthSummary &portHealth)
{
    (void)ApplyLocalClientPortHealth(portHealth);
}

std::optional<UbHealthSummary> WorkerUbHealthRegistry::GetSummary(const HostPort &worker) const
{
    auto state = std::atomic_load(&state_);
    const auto *summary = state->workers->health.Find(worker);
    return summary == nullptr ? std::nullopt : std::optional<UbHealthSummary>{ *summary };
}

bool WorkerUbHealthRegistry::IsVerifiedUnavailable(const HostPort &worker) const
{
    return std::atomic_load(&state_)->workers->verifiedUnavailable.count(worker) != 0;
}

std::shared_ptr<const UbRoutingHealthSnapshot> WorkerUbHealthRegistry::GetRoutingSnapshot() const
{
    auto state = std::atomic_load(&state_);
    return std::shared_ptr<const UbRoutingHealthSnapshot>(state, &state->routing);
}
}  // namespace datasystem::client
