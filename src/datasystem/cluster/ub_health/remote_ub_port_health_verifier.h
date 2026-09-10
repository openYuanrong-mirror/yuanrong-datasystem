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

/** Description: Coordinates deadline-external remote UB port-health verification state. */
#ifndef DATASYSTEM_CLUSTER_UB_HEALTH_REMOTE_UB_PORT_HEALTH_VERIFIER_H
#define DATASYSTEM_CLUSTER_UB_HEALTH_REMOTE_UB_PORT_HEALTH_VERIFIER_H

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "datasystem/common/object_cache/peer_ub_admission.h"

namespace datasystem::cluster {

constexpr size_t REMOTE_UB_PORT_HEALTH_MAX_CONCURRENT_QUERIES = 4;
constexpr uint64_t REMOTE_UB_PORT_HEALTH_UNSUPPORTED_RETRY_INTERVAL_MS = 30'000;
struct RemoteUbQueryTicket {
    HostPort peer;
    std::string incarnation;
    uint64_t generation = 0;
};

struct RemoteUbQueryCompletion {
    bool retryScheduled = false;
    bool evidenceAccepted = false;
};

class RemoteUbPortHealthVerifier {
public:
    explicit RemoteUbPortHealthVerifier(
        uint64_t queryIntervalMs = static_cast<uint64_t>(UB_REMOTE_PORT_HEALTH_QUERY_INTERVAL.count()));
    ~RemoteUbPortHealthVerifier() = default;

    bool RequestVerification(const HostPort &peer, const std::string &incarnation,
                             uint64_t nowMs);
    std::optional<RemoteUbQueryTicket> TryBeginDue(uint64_t nowMs);
    RemoteUbQueryCompletion Complete(const RemoteUbQueryTicket &ticket,
                                     const std::optional<UbHealthSummary> &summary,
                                     const Status &queryStatus, uint64_t nowMs);
    bool NotifySummaryHint(const UbHealthSummary &summary, uint64_t nowMs);
    void ReconcileTopology(const std::unordered_map<HostPort, std::string> &incarnations);
    std::optional<uint64_t> NextQueryDeadlineMs() const;

private:
    struct AcceptedSummaryTransition {
        bool logResponse;
        bool recoveredAfterRetry;
        const char *decision;
    };

    struct PeerState {
        std::string incarnation;
        uint64_t generation = 0;
        uint64_t nextQueryMs = 0;
        std::optional<uint64_t> lastQueryMs;
        std::optional<UbPortHealthSummary> lastPortHealth;
        bool inFlight = false;
        bool isolated = false;
        bool summaryHintPending = false;
        bool triggerPending = false;
        bool verificationPending = false;
        std::optional<StatusCode> lastLoggedRetryStatus;
    };

    void ScheduleAfterCompletion(PeerState &state, uint64_t nowMs,
                                 RemoteUbQueryCompletion &completion) const;
    AcceptedSummaryTransition ApplyAcceptedSummaryLocked(
        PeerState &state, const UbPortHealthSummary &portHealth, uint64_t nowMs,
        RemoteUbQueryCompletion &completion) const;
    void CompressIsolatedDeadlinesLocked(const HostPort &completedPeer, uint64_t nowMs);

    const uint64_t queryIntervalMs_;
    mutable std::mutex mutex_;
    std::unordered_map<HostPort, PeerState> peers_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_UB_HEALTH_REMOTE_UB_PORT_HEALTH_VERIFIER_H
