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

/** Description: Worker self port-health monitor binding for PeerUbAdmission(self). */

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_SELF_PORT_HEALTH_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_SELF_PORT_HEALTH_H

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>

#include <bthread/mutex.h>

#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

/**
 * Observes the UrmaManager context monitor and applies self facts to the worker admission. This binding never
 * owns a second provider query loop. Stop detaches this observer; UrmaManager drains the monitor before URMA teardown.
 */
class WorkerSelfPortHealth : public IUbPortHealthObserver,
                             public std::enable_shared_from_this<WorkerSelfPortHealth> {
public:
    WorkerSelfPortHealth() = default;
    ~WorkerSelfPortHealth() override;

    WorkerSelfPortHealth(const WorkerSelfPortHealth &) = delete;
    WorkerSelfPortHealth &operator=(const WorkerSelfPortHealth &) = delete;
    WorkerSelfPortHealth(WorkerSelfPortHealth &&) = delete;
    WorkerSelfPortHealth &operator=(WorkerSelfPortHealth &&) = delete;

    /**
     * Bind the admission that owns this worker's self state. Idempotent; the admission and the self address must
     * outlive this object.
     */
    void Attach(std::shared_ptr<PeerUbAdmission> admission, const HostPort &selfWorker);

    /** Bind the already-running context monitor. */
    Status Configure(std::shared_ptr<UbPortHealthMonitor> monitor,
                     std::weak_ptr<IUbPortHealthObserver> secondaryObserver = {});
    void Stop();

    /** Worker writer-side E4 evidence: request a merged local refresh; never isolates by itself. */
    void ReportPortHealthTrigger();
    std::optional<UbPortHealthSummary> GetSummary() const;
    bool IsEnabled() const;

    Status QueryUbPortHealth(const std::string &expectedWorkerIncarnation,
                             UbPortHealthSummary &summary);

    void OnUbPortHealthChanged(const UbPortHealthSummary &summary) override;

private:
    std::shared_ptr<UbPortHealthMonitor> GetMonitor() const;

    std::weak_ptr<PeerUbAdmission> admission_;
    HostPort selfWorker_;
    std::shared_ptr<UbPortHealthMonitor> monitor_;
    std::weak_ptr<IUbPortHealthObserver> secondaryObserver_;
    mutable bthread::Mutex mutex_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_SELF_PORT_HEALTH_H
