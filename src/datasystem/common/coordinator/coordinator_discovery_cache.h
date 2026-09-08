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
#ifndef DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_DISCOVERY_CACHE_H
#define DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_DISCOVERY_CACHE_H

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/utils/coordinator_discovery.h"

namespace datasystem {

class CoordinatorDiscoveryCache final {
public:
    CoordinatorDiscoveryCache(std::shared_ptr<ICoordinatorDiscovery> discovery,
                              std::vector<std::string> initialCandidates);
    ~CoordinatorDiscoveryCache();

    CoordinatorDiscoveryCache(const CoordinatorDiscoveryCache &) = delete;
    CoordinatorDiscoveryCache &operator=(const CoordinatorDiscoveryCache &) = delete;
    CoordinatorDiscoveryCache(CoordinatorDiscoveryCache &&) = delete;
    CoordinatorDiscoveryCache &operator=(CoordinatorDiscoveryCache &&) = delete;

    std::vector<std::string> GetCandidateSnapshot() const;
    void RefreshAsync();

private:
    bool WaitForRefresh();
    void Run();

    const std::shared_ptr<ICoordinatorDiscovery> discovery_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::string> candidates_;
    bool refreshRequested_{ false };
    bool stopping_{ false };
    std::thread thread_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_COORDINATOR_COORDINATOR_DISCOVERY_CACHE_H
