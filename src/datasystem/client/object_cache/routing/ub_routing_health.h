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

/** Description: Defines the UB port-health snapshot consumed by client routing. */
#ifndef DATASYSTEM_CLIENT_ROUTING_UB_ROUTING_HEALTH_H
#define DATASYSTEM_CLIENT_ROUTING_UB_ROUTING_HEALTH_H

#include <string>
#include <unordered_map>

#include "datasystem/common/object_cache/ub_port_health.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

struct WorkerUbPortHealth {
    std::string incarnation;
    UbPortHealthSummary portHealth;

    uint32_t HealthyPortCount() const noexcept
    {
        return HasKnownUbPortHealth(portHealth)
                   ? portHealth.totalPortCount - portHealth.badPortCount
                   : 0;
    }
};

struct UbRoutingHealthSnapshot {
    UbPortHealthSummary localClient;
    std::unordered_map<HostPort, WorkerUbPortHealth> workers;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_UB_ROUTING_HEALTH_H
