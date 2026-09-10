/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "datasystem/client/object_cache/discovery_backoff.h"

#include <chrono>
#include <vector>

#include "gtest/gtest.h"

namespace datasystem {
namespace object_cache {
namespace {
const auto T0 = std::chrono::steady_clock::time_point{} + std::chrono::seconds(1'000);

std::chrono::milliseconds Ms(int64_t value)
{
    return std::chrono::milliseconds(value);
}

TEST(DiscoveryBackoffTest, EscalatesExponentiallyAndCapsAtMax)
{
    DiscoveryBackoff backoff;
    EXPECT_FALSE(backoff.IsBlocked(T0));

    const std::vector<int32_t> expectedRhythmMs = { 1'000, 2'000, 4'000, 8'000, 8'000, 8'000 };
    auto probeTime = T0;
    for (const auto expectedMs : expectedRhythmMs) {
        const auto appliedMs = backoff.OnFailure(probeTime);
        ASSERT_EQ(appliedMs, expectedMs);
        ASSERT_TRUE(backoff.IsBlocked(probeTime + Ms(expectedMs - 1)));
        ASSERT_FALSE(backoff.IsBlocked(probeTime + Ms(expectedMs)));
        probeTime += Ms(expectedMs);
    }
}

TEST(DiscoveryBackoffTest, SuccessReopensGateAndRestartsRhythm)
{
    DiscoveryBackoff backoff;
    EXPECT_FALSE(backoff.OnSuccess());
    (void)backoff.OnFailure(T0);
    (void)backoff.OnFailure(T0 + Ms(1'000));
    ASSERT_TRUE(backoff.IsBlocked(T0 + Ms(2'000)));

    EXPECT_TRUE(backoff.OnSuccess());
    EXPECT_FALSE(backoff.IsBlocked(T0));
    EXPECT_FALSE(backoff.OnSuccess());
    EXPECT_EQ(backoff.OnFailure(T0), DISCOVERY_BACKOFF_INITIAL_MS);
}
}  // namespace
}  // namespace object_cache
}  // namespace datasystem
