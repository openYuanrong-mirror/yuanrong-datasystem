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

#include "datasystem/cluster/ub_health/remote_ub_port_health_verifier.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <stdexcept>
#include <utility>

#include "datasystem/common/log/log.h"

namespace datasystem::cluster {
namespace {
constexpr size_t UB_INCARNATION_LOG_PREFIX_LENGTH = 12;

bool IsUsableQuerySummary(const RemoteUbQueryTicket &ticket, const UbHealthSummary &summary)
{
    return summary.worker == ticket.peer && summary.incarnation == ticket.incarnation
           && summary.portHealth.has_value() && HasKnownUbPortHealth(*summary.portHealth)
           && !summary.portHealth->verificationPending;
}

std::string IncarnationLogPrefix(const std::string &incarnation)
{
    return incarnation.substr(0, std::min(incarnation.size(), UB_INCARNATION_LOG_PREFIX_LENGTH));
}

bool IsStaleOrConflicting(const std::optional<UbPortHealthSummary> &current,
                          const UbPortHealthSummary &incoming)
{
    if (!current.has_value()) {
        return false;
    }
    return incoming.healthEpoch < current->healthEpoch
           || (incoming.healthEpoch == current->healthEpoch
               && (incoming.totalPortCount != current->totalPortCount
                   || incoming.badPortCount != current->badPortCount));
}

Status ValidateQueryCompletion(const RemoteUbQueryTicket &ticket,
                               const std::optional<UbHealthSummary> &summary,
                               const Status &queryStatus,
                               const std::optional<UbPortHealthSummary> &lastPortHealth)
{
    if (queryStatus.IsError()) {
        return queryStatus;
    }
    if (!summary.has_value()) {
        return Status(K_INVALID, "UB port health query response has no summary");
    }
    if (!IsUsableQuerySummary(ticket, *summary)) {
        return Status(K_INVALID, "UB port health query response is unusable");
    }
    if (IsStaleOrConflicting(lastPortHealth, *summary->portHealth)) {
        return Status(K_NOT_READY, "UB port health query response is stale or conflicting");
    }
    return Status::OK();
}

void LogQueryRetry(const RemoteUbQueryTicket &ticket, const Status &status, uint64_t nextRetryMs)
{
    LOG(WARNING) << "UB_PORT_QUERY action=retry peer=" << ticket.peer.ToString()
                 << " incarnation_prefix=" << IncarnationLogPrefix(ticket.incarnation)
                 << " status_code=" << status.GetCode() << " status=" << status
                 << " next_retry_ms=" << nextRetryMs;
}

void LogQueryResponse(const RemoteUbQueryTicket &ticket, const UbPortHealthSummary &portHealth,
                      const char *decision)
{
    LOG(INFO) << "UB_PORT_QUERY action=response peer=" << ticket.peer.ToString()
              << " incarnation_prefix=" << IncarnationLogPrefix(ticket.incarnation)
              << " health_epoch=" << portHealth.healthEpoch << " bad=" << portHealth.badPortCount
              << " total=" << portHealth.totalPortCount << " decision=" << decision
              << " source=query_response";
}

}  // namespace

RemoteUbPortHealthVerifier::RemoteUbPortHealthVerifier(uint64_t queryIntervalMs)
    : queryIntervalMs_(queryIntervalMs)
{
    if (queryIntervalMs_ == 0) {
        throw std::invalid_argument("Remote UB port health query interval must be positive");
    }
}

bool RemoteUbPortHealthVerifier::RequestVerification(const HostPort &peer, const std::string &incarnation,
                                                     uint64_t nowMs)
{
    if (peer.Empty() || incarnation.empty()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto [iter, inserted] = peers_.try_emplace(peer);
    if (inserted) {
        iter->second.incarnation = incarnation;
        iter->second.nextQueryMs = nowMs;
        iter->second.verificationPending = true;
        return true;
    }
    auto &state = iter->second;
    if (state.incarnation != incarnation) {
        const uint64_t generation = state.generation + 1;
        state = PeerState{};
        state.incarnation = incarnation;
        state.generation = generation;
        state.nextQueryMs = nowMs;
        state.verificationPending = true;
        return true;
    }
    if (state.inFlight) {
        const bool firstPendingTrigger = !state.triggerPending;
        state.triggerPending = true;
        return firstPendingTrigger;
    }
    if (!state.inFlight && !state.isolated
        && state.nextQueryMs == std::numeric_limits<uint64_t>::max()) {
        state.verificationPending = true;
        state.nextQueryMs = state.lastQueryMs.has_value()
                                ? std::max(nowMs, UbProbeRetryAt(*state.lastQueryMs, queryIntervalMs_))
                                : nowMs;
        return true;
    }
    return false;
}

std::optional<RemoteUbQueryTicket> RemoteUbPortHealthVerifier::TryBeginDue(uint64_t nowMs)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto selected = peers_.end();
    for (auto iter = peers_.begin(); iter != peers_.end(); ++iter) {
        if (iter->second.inFlight || iter->second.nextQueryMs > nowMs) {
            continue;
        }
        if (selected == peers_.end() || iter->second.nextQueryMs < selected->second.nextQueryMs ||
            (iter->second.nextQueryMs == selected->second.nextQueryMs && iter->first < selected->first)) {
            selected = iter;
        }
    }
    if (selected == peers_.end()) {
        return std::nullopt;
    }
    auto &state = selected->second;
    if (!TryBeginUbProbe(nowMs, state.nextQueryMs, state.inFlight, state.generation)) {
        return std::nullopt;
    }
    state.lastQueryMs = nowMs;
    state.nextQueryMs = std::numeric_limits<uint64_t>::max();
    return RemoteUbQueryTicket{ selected->first, state.incarnation, state.generation };
}

void RemoteUbPortHealthVerifier::ScheduleAfterCompletion(
    PeerState &state, uint64_t nowMs, RemoteUbQueryCompletion &completion) const
{
    if (state.summaryHintPending) {
        state.summaryHintPending = false;
        state.triggerPending = false;
        state.nextQueryMs = nowMs;
        completion.retryScheduled = true;
    } else if (state.triggerPending) {
        state.triggerPending = false;
        state.nextQueryMs = state.lastQueryMs.has_value()
                                ? std::max(nowMs, UbProbeRetryAt(*state.lastQueryMs, queryIntervalMs_))
                                : nowMs;
        completion.retryScheduled = true;
    } else if (state.isolated || state.verificationPending) {
        state.nextQueryMs = UbProbeRetryAt(nowMs, queryIntervalMs_);
        completion.retryScheduled = true;
    } else {
        state.nextQueryMs = std::numeric_limits<uint64_t>::max();
    }
}

RemoteUbPortHealthVerifier::AcceptedSummaryTransition RemoteUbPortHealthVerifier::ApplyAcceptedSummaryLocked(
    PeerState &state, const UbPortHealthSummary &portHealth, uint64_t nowMs,
    RemoteUbQueryCompletion &completion) const
{
    const bool healthChanged = !state.lastPortHealth.has_value()
                               || !IsSameUbPortHealth(*state.lastPortHealth, portHealth);
    const bool recoveredAfterRetry = state.lastLoggedRetryStatus.has_value();
    const bool wasIsolated = state.isolated;
    const bool triggerPending = state.triggerPending;
    state.triggerPending = false;
    state.lastLoggedRetryStatus.reset();
    state.lastPortHealth = portHealth;
    state.verificationPending = triggerPending;
    completion.evidenceAccepted = true;
    if (ShouldIsolateForUbPortHealth(portHealth)) {
        state.isolated = true;
        ScheduleAfterCompletion(state, nowMs, completion);
    } else {
        state.isolated = false;
        state.summaryHintPending = false;
        if (triggerPending) {
            state.nextQueryMs = state.lastQueryMs.has_value()
                                    ? std::max(nowMs, UbProbeRetryAt(*state.lastQueryMs, queryIntervalMs_))
                                    : nowMs;
            completion.retryScheduled = true;
        } else {
            state.nextQueryMs = std::numeric_limits<uint64_t>::max();
        }
    }
    const bool logResponse = healthChanged || recoveredAfterRetry || wasIsolated != state.isolated;
    const char *decision = state.isolated ? "ISOLATE" : (wasIsolated ? "RECOVER" : "ALLOW");
    return { logResponse, recoveredAfterRetry, decision };
}

void RemoteUbPortHealthVerifier::CompressIsolatedDeadlinesLocked(const HostPort &completedPeer, uint64_t nowMs)
{
    for (auto &[peer, state] : peers_) {
        if (peer != completedPeer && state.isolated && !state.inFlight && state.nextQueryMs > nowMs) {
            state.nextQueryMs = nowMs;
        }
    }
}

RemoteUbQueryCompletion RemoteUbPortHealthVerifier::Complete(
    const RemoteUbQueryTicket &ticket, const std::optional<UbHealthSummary> &summary,
    const Status &queryStatus, uint64_t nowMs)
{
    std::unique_lock<std::mutex> lock(mutex_);
    auto iter = peers_.find(ticket.peer);
    if (iter == peers_.end() || iter->second.incarnation != ticket.incarnation
        || !MatchesUbProbe(iter->second.inFlight, iter->second.generation, ticket.generation)) {
        return {};
    }

    auto &state = iter->second;
    state.inFlight = false;
    RemoteUbQueryCompletion completion;
    auto completionStatus = ValidateQueryCompletion(ticket, summary, queryStatus, state.lastPortHealth);
    if (completionStatus.IsError()) {
        if (completionStatus.GetCode() == K_NOT_SUPPORTED) {
            state.summaryHintPending = false;
            state.triggerPending = false;
            state.nextQueryMs = UbProbeRetryAt(nowMs, REMOTE_UB_PORT_HEALTH_UNSUPPORTED_RETRY_INTERVAL_MS);
            completion.retryScheduled = true;
        } else {
            ScheduleAfterCompletion(state, nowMs, completion);
        }
        const bool logRetry = completion.retryScheduled
                              && (!state.lastLoggedRetryStatus.has_value()
                                  || *state.lastLoggedRetryStatus != completionStatus.GetCode());
        if (logRetry) {
            state.lastLoggedRetryStatus = completionStatus.GetCode();
        }
        const uint64_t nextRetryMs = state.nextQueryMs > nowMs ? state.nextQueryMs - nowMs : 0;
        lock.unlock();
        if (logRetry) {
            LogQueryRetry(ticket, completionStatus, nextRetryMs);
        }
        return completion;
    }

    const auto portHealth = *summary->portHealth;
    const auto transition = ApplyAcceptedSummaryLocked(state, portHealth, nowMs, completion);
    if (transition.recoveredAfterRetry) {
        CompressIsolatedDeadlinesLocked(ticket.peer, nowMs);
    }
    lock.unlock();
    if (transition.logResponse) {
        LogQueryResponse(ticket, portHealth, transition.decision);
    }
    return completion;
}

bool RemoteUbPortHealthVerifier::NotifySummaryHint(const UbHealthSummary &summary, uint64_t nowMs)
{
    if (!summary.portHealth.has_value() || !HasKnownUbPortHealth(*summary.portHealth)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = peers_.find(summary.worker);
    if (iter == peers_.end() || iter->second.incarnation != summary.incarnation
        || (iter->second.lastPortHealth.has_value()
            && summary.portHealth->healthEpoch <= iter->second.lastPortHealth->healthEpoch)) {
        return false;
    }
    iter->second.lastPortHealth = *summary.portHealth;
    if (iter->second.inFlight) {
        iter->second.summaryHintPending = true;
    } else if (iter->second.isolated
               || ShouldIsolateForUbPortHealth(*summary.portHealth)) {
        iter->second.nextQueryMs = nowMs;
    } else {
        return false;
    }
    return true;
}

void RemoteUbPortHealthVerifier::ReconcileTopology(
    const std::unordered_map<HostPort, std::string> &incarnations)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto iter = peers_.begin(); iter != peers_.end();) {
        auto current = incarnations.find(iter->first);
        iter = current == incarnations.end() || current->second != iter->second.incarnation
                   ? peers_.erase(iter)
                   : std::next(iter);
    }
}

std::optional<uint64_t> RemoteUbPortHealthVerifier::NextQueryDeadlineMs() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return NextUbProbeDeadline(peers_, [](const auto &entry) -> std::optional<uint64_t> {
        const auto &state = entry.second;
        return !state.inFlight && state.nextQueryMs != std::numeric_limits<uint64_t>::max()
                   ? std::optional<uint64_t>{ state.nextQueryMs } : std::nullopt;
    });
}

}  // namespace datasystem::cluster
