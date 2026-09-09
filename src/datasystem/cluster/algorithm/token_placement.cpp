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
#include "datasystem/cluster/algorithm/token_placement.h"

#include <algorithm>
#include <array>
#include <limits>
#include <utility>

#include "datasystem/cluster/algorithm/balanced_ring.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/hash_ring_token.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {
namespace {

constexpr uint64_t RING_SPACE = uint64_t{ 1 } << 32;
static_assert(BALANCED_PLACEMENT_SEED_CANDIDATES <= MAX_HASH_RING_TOKEN_SEEDS,
              "balanced placement seed candidates must stay within the probe budget");

struct TokenProbe {
    uint32_t token{ 0 };
    uint32_t seed{ 0 };
};

uint32_t CyclicDistance(uint32_t token, uint32_t target)
{
    return std::min(token - target, target - token);
}

Status ProbeUniqueToken(const MemberIdentity &identity, uint32_t index, uint32_t startSeed,
                        std::unordered_set<uint32_t> &occupied, TokenProbe &probe)
{
    for (probe.seed = startSeed; probe.seed < MAX_HASH_RING_TOKEN_SEEDS; ++probe.seed) {
        probe.token = MakeHashRingToken(identity.address, index, probe.seed);
        if (occupied.insert(probe.token).second) {
            return Status::OK();
        }
    }
    RETURN_STATUS(K_INVALID, "unique cluster token probe budget exhausted");
}

Status SelectSequentialTokens(const MemberIdentity &identity, uint32_t count, std::unordered_set<uint32_t> &occupied,
                              std::vector<uint32_t> &tokens, std::vector<TokenSeedOverride> &overrides)
{
    for (uint32_t index = 0; index < count; ++index) {
        TokenProbe probe;
        RETURN_IF_NOT_OK(ProbeUniqueToken(identity, index, 0, occupied, probe));
        tokens.emplace_back(probe.token);
        if (probe.seed > 0) {
            overrides.emplace_back(TokenSeedOverride{ index, probe.seed });
        }
    }
    return Status::OK();
}

// The nearest candidate is virtually never occupied (occupied tokens are a vanishing fraction of the ring
// space), so the occupied set is probed only for the few nearest candidates instead of every seed; the
// full scan fallback keeps the result identical to probing every candidate.
bool PickNearestToken(const std::vector<uint32_t> &candidates, uint32_t target,
                      const std::unordered_set<uint32_t> &occupied, uint32_t &bestToken, uint32_t &bestSeed)
{
    constexpr size_t nearestCandidateDepth = 4;
    std::array<std::pair<uint32_t, uint32_t>, nearestCandidateDepth> nearest{};
    size_t depth = 0;
    for (uint32_t seed = 0; seed < candidates.size(); ++seed) {
        const uint32_t distance = CyclicDistance(candidates[seed], target);
        if (depth < nearest.size() || distance < nearest[depth - 1].first) {
            if (depth < nearest.size()) {
                ++depth;
            }
            size_t slot = depth - 1;
            for (; slot > 0 && nearest[slot - 1].first > distance; --slot) {
                nearest[slot] = nearest[slot - 1];
            }
            nearest[slot] = { distance, seed };
        }
    }
    for (size_t index = 0; index < depth; ++index) {
        const uint32_t seed = nearest[index].second;
        if (occupied.count(candidates[seed]) == 0) {
            bestToken = candidates[seed];
            bestSeed = seed;
            return true;
        }
    }
    bool found = false;
    uint32_t bestDistance = std::numeric_limits<uint32_t>::max();
    for (uint32_t seed = 0; seed < candidates.size(); ++seed) {
        const uint32_t token = candidates[seed];
        if (occupied.count(token) > 0) {
            continue;
        }
        const uint32_t distance = CyclicDistance(token, target);
        if (!found || distance < bestDistance) {
            found = true;
            bestDistance = distance;
            bestSeed = seed;
            bestToken = token;
        }
    }
    return found;
}

}  // namespace

TokenPlacement::TokenPlacement(const std::vector<PlacementOwner> &owners, std::unordered_set<uint32_t> &occupied,
                               bool balanced)
    : occupied_(occupied)
{
    if (balanced) {
        ring_ = std::make_unique<BalancedRing>(owners, occupied_);
    }
}

TokenPlacement::~TokenPlacement()
{
    if (ring_ != nullptr) {
        ring_->VerifyShares();
    }
}

Status TokenPlacement::SelectTokens(const MemberIdentity &identity, uint32_t count, std::vector<uint32_t> &tokens,
                                    std::vector<TokenSeedOverride> &overrides)
{
    tokens.clear();
    tokens.reserve(count);
    overrides.clear();
    if (count == 0) {
        return Status::OK();
    }
    if (ring_ == nullptr) {
        return SelectSequentialTokens(identity, count, occupied_, tokens, overrides);
    }
    if (ring_->Empty()) {
        RETURN_IF_NOT_OK(SelectSequentialTokens(identity, count, occupied_, tokens, overrides));
        // Register the first member's tokens so later members balance against them.
        for (uint32_t token : tokens) {
            ring_->Insert(token, 0);
        }
        ring_->AdmitNewMember();
        return Status::OK();
    }
    return SelectBalancedTokens(identity, count, tokens, overrides);
}

Status TokenPlacement::SelectBalancedTokens(const MemberIdentity &identity, uint32_t count,
                                            std::vector<uint32_t> &tokens, std::vector<TokenSeedOverride> &overrides)
{
    if (count == 0) {
        return Status::OK();
    }
    const uint32_t newMember = static_cast<uint32_t>(ring_->TotalMembers()) - 1;
    const uint64_t idealPerToken = RING_SPACE / ring_->TotalMembers() / count;
    std::vector<uint32_t> candidates;
    for (uint32_t index = 0; index < count; ++index) {
        const uint32_t target = ring_->TakeTarget(ring_->HeaviestMember(), idealPerToken);
        MakeHashRingTokenCandidates(identity.address, index, BALANCED_PLACEMENT_SEED_CANDIDATES, candidates);
        uint32_t bestToken = 0;
        uint32_t bestSeed = 0;
        if (PickNearestToken(candidates, target, occupied_, bestToken, bestSeed)) {
            occupied_.insert(bestToken);
            tokens.emplace_back(bestToken);
            if (bestSeed > 0) {
                overrides.emplace_back(TokenSeedOverride{ index, bestSeed });
            }
            ring_->Insert(bestToken, newMember);
            continue;
        }
        LOG(WARNING) << "All balanced placement seed candidates collided, degrading to sequential probing for "
                     << identity.address << " token index " << index;
        TokenProbe probe;
        RETURN_IF_NOT_OK(ProbeUniqueToken(identity, index, BALANCED_PLACEMENT_SEED_CANDIDATES, occupied_, probe));
        tokens.emplace_back(probe.token);
        if (probe.seed > 0) {
            overrides.emplace_back(TokenSeedOverride{ index, probe.seed });
        }
        ring_->Insert(probe.token, newMember);
    }
    ring_->AdmitNewMember();
    return Status::OK();
}

}  // namespace datasystem::cluster
