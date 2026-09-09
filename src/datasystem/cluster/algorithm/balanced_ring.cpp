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
 * Description: Incremental balanced-ring share bookkeeping for token placement.
 */
#include "datasystem/cluster/algorithm/balanced_ring.h"

#include <algorithm>
#include <iterator>

#include "datasystem/common/log/log.h"

namespace datasystem::cluster {
namespace {
constexpr uint64_t RING_SPACE = uint64_t{ 1 } << 32;
}  // namespace

bool ArcOrder::operator()(const ArcRecord &left, const ArcRecord &right) const
{
    if (left.length != right.length) {
        return left.length > right.length;
    }
    return left.start < right.start;
}

BalancedRing::BalancedRing(const std::vector<PlacementOwner> &owners, std::unordered_set<uint32_t> &occupied)
{
    uint32_t maxMember = 0;
    for (const auto &owner : owners) {
        maxMember = std::max(maxMember, owner.memberIndex);
        ring_.emplace(owner.token, owner.memberIndex);
        occupied.insert(owner.token);
    }
    memberCount_ = owners.empty() ? 0 : static_cast<size_t>(maxMember) + 1;
    memberArcs_.resize(memberCount_ + 1);
    memberShares_.assign(memberCount_ + 1, 0);
    BuildArcs();
}

bool BalancedRing::Empty() const
{
    return ring_.empty();
}

size_t BalancedRing::TotalMembers() const
{
    return memberCount_ + 1;
}

uint32_t BalancedRing::HeaviestMember() const
{
    return static_cast<uint32_t>(std::max_element(memberShares_.begin(), memberShares_.end())
                                 - memberShares_.begin());
}

uint32_t BalancedRing::TakeTarget(uint32_t member, uint64_t idealShare) const
{
    const auto &arcs = memberArcs_[member];
    if (arcs.empty()) {
        return 0;
    }
    const ArcRecord &largest = *arcs.begin();
    if (largest.length >= idealShare) {
        return largest.start + static_cast<uint32_t>(idealShare);
    }
    return static_cast<uint32_t>(largest.start + largest.length - 1);
}

void BalancedRing::Insert(uint32_t token, uint32_t owner)
{
    if (ring_.empty()) {
        ring_.emplace(token, owner);
        AddArc(owner, ArcRecord{ RING_SPACE, token });
        return;
    }
    auto next = ring_.upper_bound(token);
    const auto prev = next == ring_.begin() ? std::prev(ring_.end()) : std::prev(next);
    const uint32_t prevToken = prev->first;
    const uint32_t nextToken = next == ring_.end() ? ring_.begin()->first : next->first;
    const uint32_t nextOwner = ring_.at(nextToken);
    const uint64_t nextLin = static_cast<uint64_t>(nextToken) + (nextToken <= prevToken ? RING_SPACE : 0);
    const uint64_t tokenLin = static_cast<uint64_t>(token) + (token <= prevToken ? RING_SPACE : 0);
    // The arc (prevToken, nextToken] belonged to nextOwner; after the split the new member owns
    // (prevToken, token] and nextOwner keeps (token, nextToken].
    const uint64_t taken = tokenLin - prevToken;
    memberArcs_[nextOwner].erase(ArcRecord{ nextLin - prevToken, prevToken });
    memberArcs_[nextOwner].insert(ArcRecord{ nextLin - tokenLin, token });
    AddArc(owner, ArcRecord{ taken, prevToken });
    memberShares_[nextOwner] -= taken;
    ring_.emplace(token, owner);
}

void BalancedRing::AdmitNewMember()
{
    memberArcs_.emplace_back();
    memberShares_.push_back(0);
    ++memberCount_;
}

void BalancedRing::VerifyShares() const
{
    std::vector<uint64_t> shares(memberShares_.size(), 0);
    for (auto iter = ring_.begin(); iter != ring_.end(); ++iter) {
        auto prev = iter == ring_.begin() ? std::prev(ring_.end()) : std::prev(iter);
        const uint64_t iterLin = static_cast<uint64_t>(iter->first)
                                 + (iter->first <= prev->first ? RING_SPACE : 0);
        shares[iter->second] += iterLin - prev->first;
    }
    if (shares != memberShares_) {
        LOG(WARNING) << "Balanced placement incremental share bookkeeping drifted from the ring";
    }
}

void BalancedRing::AddArc(uint32_t member, ArcRecord arc)
{
    memberArcs_[member].insert(arc);
    memberShares_[member] += arc.length;
}

void BalancedRing::BuildArcs()
{
    if (ring_.empty()) {
        return;
    }
    for (auto iter = ring_.begin(); iter != ring_.end(); ++iter) {
        auto prev = iter == ring_.begin() ? std::prev(ring_.end()) : std::prev(iter);
        const uint64_t iterLin = static_cast<uint64_t>(iter->first)
                                 + (iter->first <= prev->first ? RING_SPACE : 0);
        AddArc(iter->second, ArcRecord{ iterLin - prev->first, prev->first });
    }
}

}  // namespace datasystem::cluster
