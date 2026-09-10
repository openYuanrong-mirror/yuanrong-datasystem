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

#ifndef DATASYSTEM_COMMON_RDMA_CLIENT_PORT_HEALTH_ADMISSION_OBSERVER_H
#define DATASYSTEM_COMMON_RDMA_CLIENT_PORT_HEALTH_ADMISSION_OBSERVER_H

#include <atomic>
#include <cstdint>

#include "datasystem/common/object_cache/ub_port_health.h"

namespace datasystem {
constexpr uint64_t CLIENT_PORT_HEALTH_READY_MASK = 1uLL << 63;
constexpr uint64_t CLIENT_PORT_COUNT_MASK = 0x7fffffffuLL;
constexpr uint32_t CLIENT_PORT_COUNT_SHIFT = 32;

class ClientPortHealthAdmissionObserver final : public IUbPortHealthObserver {
public:
    explicit ClientPortHealthAdmissionObserver(std::atomic<uint64_t> &state) : state_(state)
    {
    }
    ~ClientPortHealthAdmissionObserver() override = default;

    void OnUbPortHealthChanged(const UbPortHealthSummary &summary) override
    {
        if (summary.verificationPending) {
            return;
        }
        const uint64_t state = !summary.valid || summary.totalPortCount == 0
                                   ? 0
                                   : CLIENT_PORT_HEALTH_READY_MASK |
                                         (static_cast<uint64_t>(summary.totalPortCount) << CLIENT_PORT_COUNT_SHIFT) |
                                         summary.badPortCount;
        state_.store(state, std::memory_order_release);
    }

private:
    std::atomic<uint64_t> &state_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RDMA_CLIENT_PORT_HEALTH_ADMISSION_OBSERVER_H
