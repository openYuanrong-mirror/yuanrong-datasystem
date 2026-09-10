/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

#include "datasystem/common/object_cache/ub_health_summary_codec.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
void EncodeUbPortHealthSummary(const UbPortHealthSummary &summary, UbPortHealthSummaryPb &pb)
{
    pb.set_valid(summary.valid);
    pb.set_total_port_count(summary.totalPortCount);
    pb.set_bad_port_count(summary.badPortCount);
    pb.set_health_epoch(summary.healthEpoch);
    pb.set_verification_pending(summary.verificationPending);
}

Status DecodeUbPortHealthSummary(const UbPortHealthSummaryPb &pb, UbPortHealthSummary &summary)
{
    UbPortHealthSummary decoded{ pb.valid(), pb.total_port_count(), pb.bad_port_count(), pb.health_epoch(),
                                 pb.verification_pending() };
    CHECK_FAIL_RETURN_STATUS(!decoded.valid || HasKnownUbPortHealth(decoded), K_INVALID,
                             "Invalid known UB port health summary");
    summary = std::move(decoded);
    return Status::OK();
}

void EncodeUbHealthSummary(const UbHealthSummary &summary, UbHealthSummaryPb &pb)
{
    pb.set_worker_address(summary.worker.ToString());
    pb.set_incarnation(summary.incarnation);
    pb.set_writable(summary.writable);
    pb.set_state(static_cast<int32_t>(summary.state));
    const auto wireReason = summary.reason == UbFailureClass::REMOTE_UNAVAILABLE_ERROR9
                                ? UbFailureClass::PORT_UNAVAILABLE_ERROR4
                                : summary.reason;
    pb.set_reason(static_cast<int32_t>(wireReason));
    pb.set_last_status_code(static_cast<int32_t>(summary.lastStatusCode));
    pb.set_epoch(summary.epoch);
    pb.set_backoff_level(summary.backoffLevel);
    pb.set_backoff_deadline_ms(summary.backoffDeadlineMs);
    pb.clear_ub_port_health();
    if (summary.portHealth.has_value()) {
        EncodeUbPortHealthSummary(*summary.portHealth, *pb.mutable_ub_port_health());
    }
}

Status DecodeUbHealthSummary(const UbHealthSummaryPb &pb, UbHealthSummary &summary)
{
    UbHealthSummary decoded;
    RETURN_IF_NOT_OK(decoded.worker.ParseString(pb.worker_address()));
    CHECK_FAIL_RETURN_STATUS(!pb.incarnation().empty(), K_INVALID, "UB health incarnation is empty");
    CHECK_FAIL_RETURN_STATUS(pb.state() >= static_cast<int32_t>(UbAdmissionState::AVAILABLE)
                                 && pb.state() <= static_cast<int32_t>(UbAdmissionState::PROBING),
                             K_INVALID, "Invalid UB admission state");
    CHECK_FAIL_RETURN_STATUS(pb.reason() >= static_cast<int32_t>(UbFailureClass::SUCCESS)
                                 && pb.reason() <= static_cast<int32_t>(UbFailureClass::NON_UB_FAILURE),
                             K_INVALID, "Invalid UB failure class");
    decoded.incarnation = pb.incarnation();
    decoded.writable = pb.writable();
    decoded.state = static_cast<UbAdmissionState>(pb.state());
    decoded.reason = static_cast<UbFailureClass>(pb.reason());
    decoded.lastStatusCode = static_cast<StatusCode>(pb.last_status_code());
    decoded.epoch = pb.epoch();
    decoded.backoffLevel = pb.backoff_level();
    decoded.backoffDeadlineMs = pb.backoff_deadline_ms();
    if (pb.has_ub_port_health()) {
        UbPortHealthSummary portHealth;
        RETURN_IF_NOT_OK(DecodeUbPortHealthSummary(pb.ub_port_health(), portHealth));
        decoded.portHealth = std::move(portHealth);
    }
    summary = std::move(decoded);
    return Status::OK();
}

Status ApplyHeartbeatUbHealthSummary(const HeartbeatRspPb &rsp, const HostPort &expectedWorker,
                                     UbHealthSummaryCache &cache,
                                     const UbHealthSummaryApplyHook &hook)
{
    if (!rsp.has_ub_health_summary()) {
        return Status::OK();
    }
    UbHealthSummary summary;
    RETURN_IF_NOT_OK(DecodeUbHealthSummary(rsp.ub_health_summary(), summary));
    CHECK_FAIL_RETURN_STATUS(summary.worker == expectedWorker, K_INVALID,
                             "UB health summary Worker does not match heartbeat source");
    // The process start id carried by HeartbeatRspPb is intentionally a different identity domain. The topology-aware
    // consumer performs the authoritative membership-incarnation fence; this cache only rejects stale epochs from the
    // same UB health incarnation before notifying that consumer.
    if (cache.Apply(summary, summary.incarnation) && hook) {
        auto accepted = cache.Get(summary.worker);
        if (accepted.has_value()) {
            hook(*accepted);
        }
    }
    return Status::OK();
}
}  // namespace datasystem
