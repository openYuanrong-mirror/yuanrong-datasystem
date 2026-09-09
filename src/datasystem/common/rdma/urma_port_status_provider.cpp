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

#include "datasystem/common/rdma/urma_port_status_provider.h"

#include <cstddef>
#include <cstdint>
#include <set>
#include <tuple>
#include <type_traits>

#ifdef USE_URMA_MOCK
#include "datasystem/common/urma_mock/abi/urma_abi_compat.h"
#else
#include <ub/umdk/urma/urma_api.h>
#include <ub/umdk/urma/urma_ubagg.h>
#endif

#include "datasystem/common/rdma/urma_dlopen_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

UrmaPortStatusProvider::UrmaPortStatusProvider(void *urmaContext) : urmaContext_(urmaContext)
{
}

Status UrmaPortStatusProvider::QueryPortStatus(UbPortHealthSnapshot &snapshot)
{
    CHECK_FAIL_RETURN_STATUS(urmaContext_ != nullptr, K_INVALID, "URMA context is null");

    bondp_query_port_status_out_t statusOut{};
    urma_user_ctl_in_t in{ .addr = 0, .len = 0, .opcode = BONDP_USER_CTL_QUERY_PORT_STATUS };
    using OutputLength = decltype(urma_user_ctl_out_t{}.len);
    urma_user_ctl_out_t out{ .addr = reinterpret_cast<uint64_t>(&statusOut),
                             .len = static_cast<OutputLength>(sizeof(statusOut)) };
    const auto ret = ds_urma_user_ctl(static_cast<urma_context_t *>(urmaContext_), &in, &out);
    if (ret != URMA_SUCCESS) {
        RETURN_STATUS(K_URMA_ERROR, FormatString("Failed to query URMA port status, ret = %d", ret));
    }

    constexpr size_t capacity = std::extent_v<decltype(statusOut.port_status)>;
    CHECK_FAIL_RETURN_STATUS(statusOut.port_count > 0 && statusOut.port_count <= capacity, K_INVALID,
                             "URMA port status output count is invalid");
    const size_t requiredLength = offsetof(bondp_query_port_status_out_t, port_status)
                                  + static_cast<size_t>(statusOut.port_count) * sizeof(statusOut.port_status[0]);
    CHECK_FAIL_RETURN_STATUS(static_cast<size_t>(out.len) >= requiredLength, K_INVALID,
                             "URMA port status output is shorter than its port count");

    UbPortHealthSnapshot candidate{ statusOut.port_count, 0 };
    std::set<std::tuple<uint32_t, uint32_t, uint32_t>> identities;
    for (uint32_t i = 0; i < statusOut.port_count; ++i) {
        const auto &port = statusOut.port_status[i];
        CHECK_FAIL_RETURN_STATUS(identities.emplace(port.chip_id, port.die_id, port.port_idx).second, K_INVALID,
                                 "URMA port status output contains a duplicate port identity");
        if (port.status == BONDP_PORT_STATUS_BAD) {
            ++candidate.badPortCount;
        } else {
            CHECK_FAIL_RETURN_STATUS(port.status == BONDP_PORT_STATUS_GOOD, K_INVALID,
                                     "URMA port status output contains an unknown port state");
        }
    }
    snapshot = candidate;
    return Status::OK();
}

}  // namespace datasystem
