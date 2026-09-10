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
 * Description: Balanced token placement advisor tests.
 */
#include "datasystem/cluster/algorithm/token_placement.h"

#include <algorithm>
#include <array>
#include <iterator>
#include <map>
#include <set>

#include "gtest/gtest.h"
#include "datasystem/cluster/algorithm/hash_algorithm.h"
#include "datasystem/common/util/hash_ring_token.h"
#include "ut/common.h"

namespace datasystem::cluster {
namespace {

constexpr uint32_t TOKENS_PER_MEMBER = 128;
constexpr uint32_t INITIAL_MEMBERS = 10;
// Fixed address groups keep the share-property assertion deterministic.
constexpr std::array<uint32_t, 10> GROUP_PORT_BASES{ 21'101, 22'203, 23'307, 24'509, 25'711,
                                                     26'317, 27'419, 28'521, 29'627, 30'729 };

MemberIdentity MakeMember(uint32_t port)
{
    std::string id(16, '\0');
    std::copy_n(reinterpret_cast<const char *>(&port), sizeof(port), id.begin());
    return { std::move(id), "10.1.1.1:" + std::to_string(port) };
}

std::vector<uint64_t> MemberArcShares(const std::vector<std::vector<uint32_t>> &tokensPerMember)
{
    // Routing semantics: the member of token T owns the arc (prev(T), T].
    std::map<uint32_t, size_t> ring;
    for (size_t member = 0; member < tokensPerMember.size(); ++member) {
        for (uint32_t token : tokensPerMember[member]) {
            ring.emplace(token, member);
        }
    }
    std::vector<uint64_t> shares(tokensPerMember.size(), 0);
    for (auto iter = ring.begin(); iter != ring.end(); ++iter) {
        auto prev = iter == ring.begin() ? std::prev(ring.end()) : std::prev(iter);
        const uint64_t iterLin =
            static_cast<uint64_t>(iter->first) + (iter->first <= prev->first ? (uint64_t{ 1 } << 32) : 0);
        shares[iter->second] += iterLin - prev->first;
    }
    return shares;
}

struct PlacementOutput {
    std::vector<uint32_t> tokens;
    std::vector<TokenSeedOverride> overrides;
};

Status PlaceMemberOnce(const MemberIdentity &identity, uint32_t count, const std::vector<PlacementOwner> &owners,
                       std::unordered_set<uint32_t> &occupied, PlacementOutput &out)
{
    TokenPlacement placement(owners, occupied, true);
    return placement.SelectTokens(identity, count, out.tokens, out.overrides);
}

struct PlacementResult {
    std::vector<std::vector<uint32_t>> tokensPerMember;
    std::vector<std::vector<TokenSeedOverride>> overridesPerMember;
    std::unordered_set<uint32_t> occupied;
};

PlacementResult PlaceGroup(uint32_t portBase, uint32_t initialMembers, uint32_t scaleOutMembers,
                           uint32_t tokensPerMember)
{
    // Initial members mirror a legacy pure-hash bootstrap ring; scale-out members are balanced.
    PlacementResult result;
    std::vector<PlacementOwner> owners;
    uint32_t nextMemberIndex = 0;
    for (uint32_t member = 0; member < initialMembers; ++member) {
        const auto identity = MakeMember(portBase + member);
        PlacementOutput out;
        DS_EXPECT_OK(PlaceMemberOnce(identity, tokensPerMember, {}, result.occupied, out));
        for (uint32_t token : out.tokens) {
            owners.push_back(PlacementOwner{ token, nextMemberIndex });
        }
        result.tokensPerMember.push_back(std::move(out.tokens));
        result.overridesPerMember.push_back(std::move(out.overrides));
        nextMemberIndex++;
    }
    TokenPlacement placement(owners, result.occupied, true);
    for (uint32_t member = 0; member < scaleOutMembers; ++member) {
        const auto identity = MakeMember(portBase + initialMembers + member);
        PlacementOutput out;
        DS_EXPECT_OK(placement.SelectTokens(identity, tokensPerMember, out.tokens, out.overrides));
        result.tokensPerMember.push_back(std::move(out.tokens));
        result.overridesPerMember.push_back(std::move(out.overrides));
    }
    return result;
}

double MaxToMinShareRatio(const std::vector<uint64_t> &shares)
{
    const auto [minShare, maxShare] = std::minmax_element(shares.begin(), shares.end());
    EXPECT_GT(*minShare, 0);
    return static_cast<double>(*maxShare) / static_cast<double>(*minShare);
}
}  // namespace

TEST(TokenPlacementTest, SelectsDeterministicTokensForIdenticalInput)
{
    const auto first = PlaceGroup(GROUP_PORT_BASES[0], INITIAL_MEMBERS, 1, 8);
    for (int run = 0; run < 3; ++run) {
        const auto repeat = PlaceGroup(GROUP_PORT_BASES[0], INITIAL_MEMBERS, 1, 8);
        ASSERT_EQ(repeat.tokensPerMember, first.tokensPerMember);
        ASSERT_EQ(repeat.overridesPerMember, first.overridesPerMember);
    }
}

TEST(TokenPlacementTest, BalancesMemberSharesForScaleOut)
{
    const auto result = PlaceGroup(GROUP_PORT_BASES[0], INITIAL_MEMBERS, 3, TOKENS_PER_MEMBER);
    ASSERT_EQ(result.tokensPerMember.size(), INITIAL_MEMBERS + 3);
    size_t tokenCount = 0;
    for (const auto &tokens : result.tokensPerMember) {
        ASSERT_EQ(tokens.size(), TOKENS_PER_MEMBER);
        tokenCount += tokens.size();
    }
    EXPECT_EQ(result.occupied.size(), tokenCount);
    const auto shares = MemberArcShares(result.tokensPerMember);
    EXPECT_LE(MaxToMinShareRatio(shares), 1.1);
}

TEST(TokenPlacementTest, BalancesSharesAcrossFixedAddressGroups)
{
    for (const auto portBase : GROUP_PORT_BASES) {
        SCOPED_TRACE("port_base=" + std::to_string(portBase));
        const auto result = PlaceGroup(portBase, INITIAL_MEMBERS, 3, TOKENS_PER_MEMBER);
        const auto shares = MemberArcShares(result.tokensPerMember);
        EXPECT_LE(MaxToMinShareRatio(shares), 1.1);
    }
}

TEST(TokenPlacementTest, CandidateDerivationMatchesPerSeedTokens)
{
    const std::string address = "10.1.1.1:21101";
    std::vector<uint32_t> candidates;
    MakeHashRingTokenCandidates(address, 7, 64, candidates);
    ASSERT_EQ(candidates.size(), 64);
    for (uint32_t seed = 0; seed < candidates.size(); ++seed) {
        EXPECT_EQ(candidates[seed], MakeHashRingToken(address, 7, seed));
    }
}

TEST(TokenPlacementTest, FirstMemberKeepsPureHashDerivation)
{
    const auto identity = MakeMember(GROUP_PORT_BASES[0]);
    std::unordered_set<uint32_t> occupied;
    PlacementOutput out;
    DS_ASSERT_OK(PlaceMemberOnce(identity, 16, {}, occupied, out));
    const auto &tokens = out.tokens;
    const auto &overrides = out.overrides;
    ASSERT_EQ(tokens.size(), 16);
    EXPECT_TRUE(overrides.empty());
    for (uint32_t index = 0; index < tokens.size(); ++index) {
        EXPECT_EQ(tokens[index], MakeHashRingToken(identity.address, index, 0));
    }
}

TEST(TokenPlacementTest, SelectsUniqueTokensWithSingleTokenPerMember)
{
    const auto result = PlaceGroup(GROUP_PORT_BASES[1], 4, 1, 1);
    EXPECT_EQ(result.occupied.size(), 5);
    for (const auto &tokens : result.tokensPerMember) {
        ASSERT_EQ(tokens.size(), 1);
    }
}

TEST(TokenPlacementTest, SessionMatchesPerMemberOneShotPlacement)
{
    std::vector<PlacementOwner> owners;
    std::unordered_set<uint32_t> oneShotOccupied;
    std::vector<std::vector<uint32_t>> oneShotTokens;
    std::vector<std::vector<TokenSeedOverride>> oneShotOverrides;
    for (uint32_t member = 0; member < 5; ++member) {
        const auto identity = MakeMember(GROUP_PORT_BASES[3] + member);
        PlacementOutput out;
        DS_EXPECT_OK(PlaceMemberOnce(identity, TOKENS_PER_MEMBER, owners, oneShotOccupied, out));
        for (uint32_t token : out.tokens) {
            owners.push_back(PlacementOwner{ token, member });
        }
        oneShotTokens.push_back(std::move(out.tokens));
        oneShotOverrides.push_back(std::move(out.overrides));
    }
    std::unordered_set<uint32_t> sessionOccupied;
    TokenPlacement placement({}, sessionOccupied, true);
    for (uint32_t member = 0; member < 5; ++member) {
        const auto identity = MakeMember(GROUP_PORT_BASES[3] + member);
        PlacementOutput out;
        DS_ASSERT_OK(placement.SelectTokens(identity, TOKENS_PER_MEMBER, out.tokens, out.overrides));
        ASSERT_EQ(out.tokens, oneShotTokens[member]);
        ASSERT_EQ(out.overrides, oneShotOverrides[member]);
    }
    EXPECT_EQ(sessionOccupied, oneShotOccupied);
}

TEST(TokenPlacementTest, DegradesToSequentialProbeWhenCandidatesCollide)
{
    const auto identity = MakeMember(GROUP_PORT_BASES[0]);
    std::vector<PlacementOwner> owners{ PlacementOwner{ 1, 0 }, PlacementOwner{ 1ULL << 31, 0 } };
    std::unordered_set<uint32_t> occupied;
    for (uint32_t seed = 0; seed < BALANCED_PLACEMENT_SEED_CANDIDATES; ++seed) {
        occupied.insert(MakeHashRingToken(identity.address, 0, seed));
    }
    PlacementOutput out;
    DS_ASSERT_OK(PlaceMemberOnce(identity, 1, owners, occupied, out));
    ASSERT_EQ(out.tokens.size(), 1);
    EXPECT_EQ(out.tokens[0], MakeHashRingToken(identity.address, 0, BALANCED_PLACEMENT_SEED_CANDIDATES));
    ASSERT_EQ(out.overrides.size(), 1);
    EXPECT_EQ(out.overrides[0].tokenIndex, 0);
    EXPECT_EQ(out.overrides[0].tokenSeed, BALANCED_PLACEMENT_SEED_CANDIDATES);
}

TEST(TokenPlacementTest, FailsWhenProbeBudgetIsExhausted)
{
    const auto identity = MakeMember(GROUP_PORT_BASES[0]);
    std::vector<PlacementOwner> owners{ PlacementOwner{ 1, 0 } };
    std::unordered_set<uint32_t> occupied;
    for (uint32_t seed = 0; seed < MAX_HASH_RING_TOKEN_SEEDS; ++seed) {
        occupied.insert(MakeHashRingToken(identity.address, 0, seed));
    }
    PlacementOutput out;
    const Status status = PlaceMemberOnce(identity, 1, owners, occupied, out);
    EXPECT_EQ(status.GetCode(), K_INVALID);
    EXPECT_TRUE(out.tokens.empty());
}

TEST(TokenPlacementTest, BuildInitialPlacementBalancesBootstrapMembers)
{
    HashAlgorithm algorithm;
    ScaleOutPlanInput input;
    input.tokensPerMember = 32;
    input.joining.reserve(INITIAL_MEMBERS);
    for (uint32_t member = 0; member < INITIAL_MEMBERS; ++member) {
        input.joining.emplace_back(MakeMember(GROUP_PORT_BASES[2] + member));
    }
    TopologyPlan plan;
    DS_ASSERT_OK(algorithm.BuildInitialPlacement(input, plan));
    ASSERT_EQ(plan.next.members.size(), INITIAL_MEMBERS);
    std::vector<std::vector<uint32_t>> tokensPerMember;
    std::set<uint32_t> uniqueTokens;
    for (const auto &member : plan.next.members) {
        ASSERT_EQ(member.tokens.size(), input.tokensPerMember);
        uniqueTokens.insert(member.tokens.begin(), member.tokens.end());
        tokensPerMember.push_back(member.tokens);
    }
    EXPECT_EQ(uniqueTokens.size(), INITIAL_MEMBERS * input.tokensPerMember);
    // Members are planned in address order: the first one is pure hash, later ones are balanced.
    for (uint32_t index = 0; index < plan.next.members.front().tokens.size(); ++index) {
        EXPECT_EQ(plan.next.members.front().tokens[index],
                  HashAlgorithm::MakeToken(plan.next.members.front().identity.address, index, 0));
    }
    EXPECT_LE(MaxToMinShareRatio(MemberArcShares(tokensPerMember)), 1.1);
}

}  // namespace datasystem::cluster
