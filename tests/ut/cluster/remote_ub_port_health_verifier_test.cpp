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

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/cluster/ub_health/remote_ub_port_health_verifier.h"
namespace datasystem::cluster {
namespace {
const HostPort WORKER("127.0.0.1", 18480);
constexpr char INCARNATION[] = "worker-incarnation";

UbHealthSummary Summary(uint32_t totalPorts, uint32_t badPorts, uint64_t healthEpoch,
                        bool pending = false)
{
    UbHealthSummary summary;
    summary.worker = WORKER;
    summary.incarnation = INCARNATION;
    summary.portHealth = UbPortHealthSummary{ true, totalPorts, badPorts, healthEpoch, pending };
    return summary;
}

UbHealthSummary SummaryFor(const HostPort &worker, const std::string &incarnation,
                           uint32_t totalPorts, uint32_t badPorts, uint64_t healthEpoch)
{
    auto summary = Summary(totalPorts, badPorts, healthEpoch);
    summary.worker = worker;
    summary.incarnation = incarnation;
    return summary;
}

}  // namespace

TEST(RemoteUbPortHealthVerifierTest, ClientModeQueriesEverySecondUntilAnyPortRecovers)
{
    RemoteUbPortHealthVerifier verifier;
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));

    auto first = verifier.TryBeginDue(100);
    ASSERT_TRUE(first.has_value());
    auto isolated = verifier.Complete(*first, Summary(4, 4, 1), Status::OK(), 120);
    EXPECT_TRUE(isolated.evidenceAccepted);
    EXPECT_TRUE(isolated.retryScheduled);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'120u);
    EXPECT_FALSE(verifier.RequestVerification(
        WORKER, INCARNATION, 200));
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'120u);
    EXPECT_FALSE(verifier.TryBeginDue(1'119).has_value());

    auto second = verifier.TryBeginDue(1'120);
    ASSERT_TRUE(second.has_value());
    auto recovered = verifier.Complete(*second, Summary(4, 3, 2), Status::OK(), 1'140);
    EXPECT_TRUE(recovered.evidenceAccepted);
    EXPECT_FALSE(recovered.retryScheduled);
    EXPECT_FALSE(verifier.NextQueryDeadlineMs().has_value());
}

TEST(RemoteUbPortHealthVerifierTest, QueryFailureRetriesUntilFirstUsablePortFact)
{
    RemoteUbPortHealthVerifier clientVerifier;
    ASSERT_TRUE(clientVerifier.RequestVerification(
        WORKER, INCARNATION, 0));
    auto clientTicket = clientVerifier.TryBeginDue(0);
    ASSERT_TRUE(clientTicket.has_value());
    auto clientResult = clientVerifier.Complete(
        *clientTicket, std::nullopt, Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 10);
    EXPECT_FALSE(clientResult.evidenceAccepted);
    EXPECT_TRUE(clientResult.retryScheduled);
    EXPECT_EQ(clientVerifier.NextQueryDeadlineMs(), 1'010u);

    auto isolationTicket = clientVerifier.TryBeginDue(1'010);
    ASSERT_TRUE(isolationTicket.has_value());
    auto isolated = clientVerifier.Complete(*isolationTicket, Summary(4, 4, 1), Status::OK(), 1'020);
    EXPECT_TRUE(isolated.evidenceAccepted);
    EXPECT_TRUE(isolated.retryScheduled);
    auto recoveryTicket = clientVerifier.TryBeginDue(2'020);
    ASSERT_TRUE(recoveryTicket.has_value());
    auto recoveryFailure = clientVerifier.Complete(
        *recoveryTicket, std::nullopt, Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 2'030);
    EXPECT_TRUE(recoveryFailure.retryScheduled);
    EXPECT_EQ(clientVerifier.NextQueryDeadlineMs(), 3'030u);

    RemoteUbPortHealthVerifier workerVerifier;
    ASSERT_TRUE(workerVerifier.RequestVerification(
        WORKER, INCARNATION, 0));
    auto workerTicket = workerVerifier.TryBeginDue(0);
    ASSERT_TRUE(workerTicket.has_value());
    auto workerResult = workerVerifier.Complete(
        *workerTicket, std::nullopt, Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 10);
    EXPECT_TRUE(workerResult.retryScheduled);
    EXPECT_EQ(workerVerifier.NextQueryDeadlineMs(), 1'010u);
    EXPECT_FALSE(workerVerifier.RequestVerification(
        WORKER, INCARNATION, 999));
    EXPECT_FALSE(workerVerifier.RequestVerification(
        WORKER, INCARNATION, 1'000));
    EXPECT_EQ(workerVerifier.NextQueryDeadlineMs(), 1'010u);
}

TEST(RemoteUbPortHealthVerifierTest, ConcurrentTriggersProduceOneInFlightQuery)
{
    RemoteUbPortHealthVerifier verifier;
    std::atomic<size_t> acceptedRequests{ 0 };
    std::vector<std::thread> requesters;
    for (size_t index = 0; index < 16; ++index) {
        requesters.emplace_back([&] {
            if (verifier.RequestVerification(
                    WORKER, INCARNATION, 0)) {
                acceptedRequests.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto &requester : requesters) {
        requester.join();
    }
    EXPECT_EQ(acceptedRequests.load(std::memory_order_relaxed), 1u);

    std::atomic<size_t> tickets{ 0 };
    std::vector<std::thread> dispatchers;
    for (size_t index = 0; index < 16; ++index) {
        dispatchers.emplace_back([&] {
            if (verifier.TryBeginDue(0).has_value()) {
                tickets.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto &dispatcher : dispatchers) {
        dispatcher.join();
    }
    EXPECT_EQ(tickets.load(std::memory_order_relaxed), 1u);
}

TEST(RemoteUbPortHealthVerifierTest, TriggerDuringQuerySchedulesOneRateLimitedFollowUp)
{
    RemoteUbPortHealthVerifier verifier;
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));
    auto ticket = verifier.TryBeginDue(100);
    ASSERT_TRUE(ticket.has_value());

    EXPECT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 150));
    EXPECT_FALSE(verifier.RequestVerification(WORKER, INCARNATION, 160));
    auto completion = verifier.Complete(*ticket, Summary(4, 0, 1), Status::OK(), 200);

    EXPECT_TRUE(completion.retryScheduled);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'100u);
}

TEST(RemoteUbPortHealthVerifierTest, UnsupportedPeerUsesBoundedBackoff)
{
    RemoteUbPortHealthVerifier verifier;
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 100));
    auto ticket = verifier.TryBeginDue(100);
    ASSERT_TRUE(ticket.has_value());

    auto completion = verifier.Complete(
        *ticket, std::nullopt, Status(K_NOT_SUPPORTED, "old Worker"), 120);

    EXPECT_TRUE(completion.retryScheduled);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 30'120u);
}

TEST(RemoteUbPortHealthVerifierTest, SuccessfulRetryCompressesOtherIsolationDeadlines)
{
    const HostPort secondWorker("127.0.0.1", 18481);
    constexpr char secondIncarnation[] = "second-incarnation";
    RemoteUbPortHealthVerifier verifier;
    ASSERT_TRUE(verifier.RequestVerification(WORKER, INCARNATION, 0));

    auto failedTicket = verifier.TryBeginDue(0);
    ASSERT_TRUE(failedTicket.has_value());
    ASSERT_EQ(failedTicket->peer, WORKER);
    ASSERT_TRUE(verifier.RequestVerification(secondWorker, secondIncarnation, 0));
    verifier.Complete(*failedTicket, std::nullopt, Status(K_RPC_DEADLINE_EXCEEDED, "timeout"), 10);

    auto isolatedTicket = verifier.TryBeginDue(10);
    ASSERT_TRUE(isolatedTicket.has_value());
    ASSERT_EQ(isolatedTicket->peer, secondWorker);
    verifier.Complete(*isolatedTicket,
                      SummaryFor(secondWorker, secondIncarnation, 4, 4, 1), Status::OK(), 120);
    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'010u);

    auto retry = verifier.TryBeginDue(1'010);
    ASSERT_TRUE(retry.has_value());
    ASSERT_EQ(retry->peer, WORKER);
    verifier.Complete(*retry, Summary(4, 0, 1), Status::OK(), 1'020);

    EXPECT_EQ(verifier.NextQueryDeadlineMs(), 1'020u);
}

}  // namespace datasystem::cluster
