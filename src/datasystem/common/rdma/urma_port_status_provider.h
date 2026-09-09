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

/** Description: URMA adapter for querying local bonding port status. */

#ifndef DATASYSTEM_COMMON_RDMA_URMA_PORT_STATUS_PROVIDER_H
#define DATASYSTEM_COMMON_RDMA_URMA_PORT_STATUS_PROVIDER_H

#include "datasystem/common/rdma/ub_port_health.h"

namespace datasystem {

class UrmaPortStatusProvider : public IUbPortStatusProvider {
public:
    explicit UrmaPortStatusProvider(void *urmaContext);
    ~UrmaPortStatusProvider() override = default;

    Status QueryPortStatus(UbPortHealthSnapshot &snapshot) override;

private:
    void *urmaContext_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_URMA_PORT_STATUS_PROVIDER_H
