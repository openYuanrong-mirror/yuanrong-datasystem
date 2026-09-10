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

/** Description: URMA vendor port-status adapter for the process-local UB port health monitor. */

#ifndef DATASYSTEM_COMMON_RDMA_URMA_PORT_STATUS_PROVIDER_H
#define DATASYSTEM_COMMON_RDMA_URMA_PORT_STATUS_PROVIDER_H

#include "datasystem/common/object_cache/ub_port_health.h"

namespace datasystem {

/**
 * Adapts the process-local URMA user-ctl port-status query to vendor-neutral states. A health monitor is the caller;
 * business paths must not query this adapter directly. The context handle is borrowed for the provider lifetime;
 * its owner must outlive this provider. Vendor types stay inside the implementation file so that business code
 * never depends on the URMA ABI.
 */
class UrmaPortStatusProvider : public IUbPortStatusProvider {
public:
    explicit UrmaPortStatusProvider(void *urmaContext);
    ~UrmaPortStatusProvider() override = default;

    UrmaPortStatusProvider(const UrmaPortStatusProvider &) = delete;
    UrmaPortStatusProvider &operator=(const UrmaPortStatusProvider &) = delete;
    UrmaPortStatusProvider(UrmaPortStatusProvider &&) = delete;
    UrmaPortStatusProvider &operator=(UrmaPortStatusProvider &&) = delete;

    Status QueryPortStatus(std::vector<UbPortStatus> &portStatus) override;

private:
    void *urmaContext_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_URMA_PORT_STATUS_PROVIDER_H
