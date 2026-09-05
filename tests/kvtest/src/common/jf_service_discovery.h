#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/utils/coordinator_discovery.h"
#include "datasystem/utils/status.h"

#include "vendor/httplib.h"
#include "vendor/nlohmann_json.hpp"

namespace kvtest {

using json = nlohmann::json;

class JfClient {
public:
    explicit JfClient(const std::string &jfServerAddr, int defaultTtl = 30)
        : jfAddr_(jfServerAddr), defaultTtl_(defaultTtl)
    {
    }

    ~JfClient()
    {
        StopAllHeartbeats();
    }

    JfClient(const JfClient &) = delete;
    JfClient &operator=(const JfClient &) = delete;

    datasystem::Status RegisterService(const std::string &serviceName, int port)
    {
        std::string ip = DetectLocalIp();
        json body = { { "service", serviceName }, { "port", port }, { "ttl", defaultTtl_ } };
        std::string resp;
        auto rc = HttpPost("/register", body.dump(), resp);
        if (!rc.IsOk())
            return rc;
        try {
            auto j = json::parse(resp);
            if (!j.value("ok", false)) {
                return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                          "JF register failed: " + j.value("error", "unknown"));
            }
        } catch (const std::exception &e) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                      std::string("JF register response parse error: ") + e.what());
        }
        StartHeartbeat(serviceName, ip, port);
        return datasystem::Status::OK();
    }

    datasystem::Status UnregisterService(const std::string &serviceName, int port)
    {
        std::string ip = DetectLocalIp();
        StopHeartbeat(serviceName, ip, port);
        json body = { { "service", serviceName }, { "port", port } };
        std::string resp;
        auto rc = HttpPost("/unregister", body.dump(), resp);
        if (!rc.IsOk())
            return rc;
        return datasystem::Status::OK();
    }

    datasystem::Status GetInstance(const std::string &serviceName, std::vector<std::string> &instances)
    {
        std::string resp;
        auto rc = HttpGet("/discover/" + serviceName, resp);
        if (!rc.IsOk())
            return rc;
        try {
            auto j = json::parse(resp);
            instances.clear();
            for (auto &addr : j["instances"]) {
                instances.push_back(addr.get<std::string>());
            }
        } catch (const std::exception &e) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                      std::string("JF discover response parse error: ") + e.what());
        }
        return datasystem::Status::OK();
    }

private:
    void StartHeartbeat(const std::string &service, const std::string &ip, int port)
    {
        std::string key = service + ":" + ip + ":" + std::to_string(port);
        std::lock_guard<std::mutex> lock(mutex_);
        if (heartbeatThreads_.count(key) > 0)
            return;
        heartbeatThreads_[key] = std::thread([this, service, ip, port]() {
            // ttl/6 (was ttl/3) tolerates two consecutive missed heartbeats
            // before TTL expiry. On a 1-core host the leader is CPU-starved
            // by brpc + SHA-256 recovery + watch fan-out after winning
            // election; the heartbeat pthread can be preempted >10s, so the
            // old ttl/3=10s interval left zero slack after one slow round.
            int interval = defaultTtl_ / 6;
            if (interval < 1) {
                interval = 1;
            }
            while (running_.load()) {
                {
                    std::unique_lock<std::mutex> lk(mutex_);
                    if (cv_.wait_for(lk, std::chrono::seconds(interval), [this] { return !running_.load(); })) {
                        break;
                    }
                }
                json body = { { "service", service }, { "port", port } };
                std::string resp;
                auto rc = HttpPost("/heartbeat", body.dump(), resp);
                if (rc.IsError()) {
                    fprintf(stderr, "JF heartbeat failed for %s:%d: %s\n", service.c_str(), port,
                            rc.ToString().c_str());
                    // Immediate retry before waiting another full interval:
                    // breaks the "slow round -> wait interval -> slow round
                    // -> TTL gone" positive feedback that expires the
                    // coordinator under worker-startup CPU saturation.
                    std::unique_lock<std::mutex> lk(mutex_);
                    if (cv_.wait_for(lk, std::chrono::seconds(1), [this] { return !running_.load(); })) {
                        break;
                    }
                    auto rc2 = HttpPost("/heartbeat", body.dump(), resp);
                    if (rc2.IsError()) {
                        fprintf(stderr, "JF heartbeat retry failed for %s:%d: %s\n", service.c_str(), port,
                                rc2.ToString().c_str());
                    }
                }
            }
        });
    }

    void StopHeartbeat(const std::string &service, const std::string &ip, int port)
    {
        std::string key = service + ":" + ip + ":" + std::to_string(port);
        std::thread *t = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = heartbeatThreads_.find(key);
            if (it != heartbeatThreads_.end()) {
                t = &it->second;
            }
        }
        if (t) {
            running_.store(false);
            cv_.notify_all();
            if (t->joinable())
                t->join();
            std::lock_guard<std::mutex> lock(mutex_);
            heartbeatThreads_.erase(key);
        }
    }

    void StopAllHeartbeats()
    {
        running_.store(false);
        cv_.notify_all();
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto &pair : heartbeatThreads_) {
            if (pair.second.joinable())
                pair.second.join();
        }
        heartbeatThreads_.clear();
    }

    datasystem::Status HttpPost(const std::string &path, const std::string &body, std::string &resp)
    {
        auto cli = CreateClient();
        if (!cli) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR, "Cannot connect to JF server: " + jfAddr_);
        }
        auto res = cli->Post(path, body, "application/json");
        if (!res) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR, "JF POST " + path + " failed: no response");
        }
        if (res->status != 200) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                      "JF POST " + path + " returned status " + std::to_string(res->status));
        }
        resp = res->body;
        return datasystem::Status::OK();
    }

    datasystem::Status HttpGet(const std::string &path, std::string &resp)
    {
        auto cli = CreateClient();
        if (!cli) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR, "Cannot connect to JF server: " + jfAddr_);
        }
        auto res = cli->Get(path);
        if (!res) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR, "JF GET " + path + " failed: no response");
        }
        if (res->status != 200) {
            return datasystem::Status(datasystem::K_RUNTIME_ERROR,
                                      "JF GET " + path + " returned status " + std::to_string(res->status));
        }
        resp = res->body;
        return datasystem::Status::OK();
    }

    std::unique_ptr<httplib::Client> CreateClient()
    {
        auto pos = jfAddr_.find(':');
        std::string host = (pos != std::string::npos) ? jfAddr_.substr(0, pos) : jfAddr_;
        int port = (pos != std::string::npos) ? std::stoi(jfAddr_.substr(pos + 1)) : 80;
        auto cli = std::make_unique<httplib::Client>(host.c_str(), port);
        // Without explicit timeouts, httplib::Client can block on connect()
        // for tens of seconds when the JF server's listen backlog overflows
        // (2000-worker startup burst). The coordinator heartbeat thread
        // would then miss TTL and be expired. 2s connect / 5s read bounds
        // a single failed heartbeat round so the next interval (TTL/3)
        // can retry, instead of blocking past TTL.
        cli->set_connection_timeout(2);
        cli->set_read_timeout(5);
        return cli;
    }

    static std::string DetectLocalIp()
    {
        const char *podIp = std::getenv("POD_IP");
        if (podIp && *podIp)
            return podIp;
        const char *hostIp = std::getenv("HOST_IP");
        if (hostIp && *hostIp)
            return hostIp;
        return "127.0.0.1";
    }

    std::string jfAddr_;
    int defaultTtl_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{ true };
    std::map<std::string, std::thread> heartbeatThreads_;
};

class UserCoordinatorDiscovery : public datasystem::ICoordinatorDiscovery {
public:
    UserCoordinatorDiscovery(std::shared_ptr<JfClient> jfClient, std::string serviceName)
        : jfClient_(std::move(jfClient)), serviceName_(std::move(serviceName))
    {
    }

    datasystem::Status GetCoordinators(std::vector<std::string> &serviceList) override
    {
        return jfClient_->GetInstance(serviceName_, serviceList);
    }

private:
    std::shared_ptr<JfClient> jfClient_;
    std::string serviceName_;
};

}  // namespace kvtest
