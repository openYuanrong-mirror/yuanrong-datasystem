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

/**
 * Description: Balanced hash-ring token placement for new topology members.
 */
#ifndef DATASYSTEM_CLUSTER_ALGORITHM_TOKEN_PLACEMENT_H
#define DATASYSTEM_CLUSTER_ALGORITHM_TOKEN_PLACEMENT_H

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include "datasystem/cluster/algorithm/balanced_ring.h"
#include "datasystem/cluster/model/topology_types.h"
#include "datasystem/utils/status.h"

namespace datasystem::cluster {

/**
 * @brief Plan-scoped placement: admits new ring members one by one.
 *
 * The balanced ring is built once from the existing owners and then maintained incrementally, so a
 * plan admitting many members pays one ring build instead of one per member. Each token takes an
 * ideal-share prefix of the currently heaviest member's largest arc, and the seed candidate nearest
 * to that target wins (see BALANCED_PLACEMENT_SEED_CANDIDATES). With no owners the first member
 * degrades to pure hash derivation with sequential collision probing.
 */
class TokenPlacement final {
public:
    /**
     * @param[in] owners Current ring owners sorted or unsorted; may be empty for a fresh ring.
     * @param[in,out] occupied Occupied token set; owner tokens are inserted and every selected token
     *                  is inserted as well.
     * @param[in] balanced False to degrade to pure hash derivation for every member; callers pass
     *                  the plan-level decision so every member in one plan is placed uniformly.
     */
    TokenPlacement(const std::vector<PlacementOwner> &owners, std::unordered_set<uint32_t> &occupied,
                   bool balanced);

    ~TokenPlacement();

    TokenPlacement(const TokenPlacement &) = delete;
    TokenPlacement &operator=(const TokenPlacement &) = delete;

    /**
     * @brief Select deterministic, share-balanced tokens for one new ring member.
     *
     * @param[in] identity New member identity; only the address drives token derivation.
     * @param[in] count Token count for the member.
     * @param[out] tokens Selected tokens in token-index order.
     * @param[out] overrides Seed overrides for tokens whose selected seed is non-zero.
     * @return K_OK, or K_INVALID when the collision probe budget is exhausted.
     */
    Status SelectTokens(const MemberIdentity &identity, uint32_t count, std::vector<uint32_t> &tokens,
                        std::vector<TokenSeedOverride> &overrides);

private:
    Status SelectBalancedTokens(const MemberIdentity &identity, uint32_t count, std::vector<uint32_t> &tokens,
                                std::vector<TokenSeedOverride> &overrides);

    std::unordered_set<uint32_t> &occupied_;
    std::unique_ptr<BalancedRing> ring_;
};

}  // namespace datasystem::cluster

#endif  // DATASYSTEM_CLUSTER_ALGORITHM_TOKEN_PLACEMENT_H
