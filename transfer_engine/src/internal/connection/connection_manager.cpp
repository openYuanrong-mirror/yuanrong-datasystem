#include "internal/connection/connection_manager.h"

#include <sstream>

namespace datasystem {
namespace {

constexpr size_t K_MAX_CONNECTION_STATES = 4096;

}  // namespace

std::string ConnectionManager::ToMapKey(const ConnectionKey &key)
{
    std::ostringstream oss;
    oss << key.localDeviceId << "|" << key.peerHost << "|" << key.peerPort << "|" << key.peerDeviceId;
    return oss.str();
}

bool ConnectionManager::HasReadyConnection(const ConnectionKey &key) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iter = states_.find(ToMapKey(key));
    if (iter == states_.end()) {
        return false;
    }
    return iter->second.requesterRecvReady && iter->second.ownerSendReady && !iter->second.stale;
}

ConnectionState ConnectionManager::GetState(const ConnectionKey &key) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iter = states_.find(ToMapKey(key));
    return iter == states_.end() ? ConnectionState{} : iter->second;
}

void ConnectionManager::MarkStale(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    states_.erase(ToMapKey(key));
}

void ConnectionManager::MarkRequesterRecvReady(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto &state = GetOrCreateStateLocked(ToMapKey(key));
    state.requesterRecvReady = true;
    state.stale = false;
}

void ConnectionManager::MarkOwnerSendReady(const ConnectionKey &key)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto &state = GetOrCreateStateLocked(ToMapKey(key));
    state.ownerSendReady = true;
    state.stale = false;
}

void ConnectionManager::Clear()
{
    std::lock_guard<std::mutex> lock(mutex_);
    states_.clear();
}

size_t ConnectionManager::Size() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return states_.size();
}

ConnectionState &ConnectionManager::GetOrCreateStateLocked(const std::string &mapKey)
{
    const auto existing = states_.find(mapKey);
    if (existing != states_.end()) {
        return existing->second;
    }
    if (states_.size() >= K_MAX_CONNECTION_STATES) {
        states_.erase(states_.begin());
    }
    return states_[mapKey];
}

}  // namespace datasystem
