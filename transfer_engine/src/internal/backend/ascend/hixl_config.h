/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#ifndef TRANSFER_ENGINE_INTERNAL_HIXL_CONFIG_H
#define TRANSFER_ENGINE_INTERNAL_HIXL_CONFIG_H

#include <string>

#include "datasystem/transfer_engine/status.h"

namespace datasystem {

constexpr char K_DEFAULT_HIXL_ROUTE[] = "roce";
constexpr char K_DEFAULT_HIXL_CS_MODE[] = "on";

enum class HixlEngineMode { kLegacy, kClientServer };

struct HixlCsConfigInput {
    std::string requestedMode;
    std::string routePolicy;
    std::string localCommRes;
    std::string globalResourceConfig;
    bool capabilityAvailable = false;
    bool legacyRoceEnabled = false;
};

struct HixlCsConfig {
    HixlEngineMode engineMode = HixlEngineMode::kLegacy;
    std::string localCommRes;
    std::string globalResourceConfig;
};

struct HixlAutoConnectConfig {
    bool enabled = false;
    std::string optionValue;
};

struct HixlPeerInfo {
    std::string backendKind;
    std::string endpoint;
    std::string routePolicy;
    HixlEngineMode engineMode = HixlEngineMode::kLegacy;
};

const char *HixlEngineModeName(HixlEngineMode mode);
Result ResolveHixlCsConfig(const HixlCsConfigInput &input, HixlCsConfig *config);
Result ResolveHixlAutoConnectConfig(const std::string &requestedMode, bool capabilityAvailable,
                                    HixlAutoConnectConfig *config);
Result ParseHixlPeerInfo(const std::string &rootInfoBytes, HixlPeerInfo *peerInfo);
std::string EncodeHixlPeerInfo(const HixlPeerInfo &peerInfo);

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_HIXL_CONFIG_H
