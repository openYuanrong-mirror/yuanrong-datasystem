/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include "datasystem/common/coordinator/coordinator_discovery_cache.h"

#include <atomic>
#include <chrono>
#include <thread>

#include "gtest/gtest.h"

namespace datasystem {
namespace {

class RefreshDiscovery final : public ICoordinatorDiscovery {
public:
    Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        ++calls_;
        serviceList = { "127.0.0.1:30002", "invalid", "127.0.0.1:30002" };
        return Status::OK();
    }

    size_t Calls() const
    {
        return calls_.load();
    }

private:
    std::atomic<size_t> calls_{ 0 };
};

TEST(CoordinatorDiscoveryCacheTest, RefreshesSnapshotOffTheCallingThread)
{
    auto discovery = std::make_shared<RefreshDiscovery>();
    CoordinatorDiscoveryCache cache(discovery, { "127.0.0.1:30001" });

    cache.RefreshAsync();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (cache.GetCandidateSnapshot() != std::vector<std::string>{ "127.0.0.1:30002" }
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    EXPECT_EQ(discovery->Calls(), 1);
    EXPECT_EQ(cache.GetCandidateSnapshot(), (std::vector<std::string>{ "127.0.0.1:30002" }));
}

}  // namespace
}  // namespace datasystem
