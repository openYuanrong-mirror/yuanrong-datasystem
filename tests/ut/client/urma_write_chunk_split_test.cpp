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

// Covers the UrmaWriteImpl chunking contract: when the configured max write size (bounded by the
// device cap) forces a split, the payload must be divided into the minimum number of chunks with
// even per-chunk sizes (8MB/5MB limit -> 4+4, not 5+3). Runs against the URMA mock backend so the
// post/wait path, event registration, and chunk traces are exercised without a real device.

#include <gtest/gtest.h>

#include <sys/mman.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#ifdef USE_URMA
#define private public
#define protected public
#include "datasystem/common/rdma/urma_manager.h"
#undef protected
#undef private

#include "datasystem/common/rdma/urma_resource.h"
#include "datasystem/common/util/raii.h"

DS_DECLARE_bool(enable_urma);
DS_DECLARE_uint64(urma_max_write_size_mb);
DS_DECLARE_uint32(urma_send_jetty_lane_pool_size);
DS_DECLARE_uint32(urma_send_jetty_lane_refill_extra_size);

namespace datasystem {
namespace {
constexpr uint64_t kMb = 1024ULL * 1024ULL;
// Any 16-byte EID is accepted by the legacy handshake import path; the mock resolves the target
// segment lazily at post time, so no real peer registration is needed.
constexpr const char *kFakeEid = "0123456789abcdef";
constexpr const char *kRemoteAddress = "127.0.0.1";
constexpr uint32_t kRemotePort = 29300;
constexpr uint64_t kRemoteSegVa = 0x10000;
constexpr int64_t kWriteWaitTimeoutMs = 10'000;

class UrmaWriteChunkSplitTest : public testing::Test {
protected:
    void SetUp() override
    {
        if (!IsMockBackendAvailable()) {
            GTEST_SKIP() << "Chunk-split coverage requires the URMA mock backend (--config=urma_mock).";
        }
        const auto savedEnableUrma = FLAGS_enable_urma;
        const auto savedMaxWriteSizeMb = FLAGS_urma_max_write_size_mb;
        const auto savedLanePoolSize = FLAGS_urma_send_jetty_lane_pool_size;
        const auto savedLaneRefillExtraSize = FLAGS_urma_send_jetty_lane_refill_extra_size;
        flagRestore_.AddTask([savedEnableUrma] { FLAGS_enable_urma = savedEnableUrma; });
        flagRestore_.AddTask([savedMaxWriteSizeMb] { FLAGS_urma_max_write_size_mb = savedMaxWriteSizeMb; });
        flagRestore_.AddTask([savedLanePoolSize] { FLAGS_urma_send_jetty_lane_pool_size = savedLanePoolSize; });
        flagRestore_.AddTask([savedLaneRefillExtraSize] {
            FLAGS_urma_send_jetty_lane_refill_extra_size = savedLaneRefillExtraSize;
        });
        FLAGS_enable_urma = true;
        FLAGS_urma_max_write_size_mb = 1;
        FLAGS_urma_send_jetty_lane_pool_size = 4;
        FLAGS_urma_send_jetty_lane_refill_extra_size = 4;

        auto &manager = UrmaManager::Instance();
        ASSERT_TRUE(manager.Init(HostPort("127.0.0.1", 0)).IsOk()) << "UrmaManager::Init failed on mock backend";
        ASSERT_NE(manager.urmaResource_, nullptr);
        // The mock device cap (1GB) is far above the 1MB flag, so the flag is the effective limit.
        maxWriteSize_ = manager.urmaResource_->GetMaxWriteSize();
        ASSERT_EQ(maxWriteSize_, kMb) << "expected the 1MB flag to be the effective max write size";

        UrmaHandshakeReqPb handshake;
        handshake.set_eid(kFakeEid);
        handshake.set_uasid(1);
        handshake.add_jfr_ids(1);
        handshake.mutable_address()->set_host(kRemoteAddress);
        handshake.mutable_address()->set_port(kRemotePort);
        handshake.set_urma_instance_id("chunk-split-test-peer");
        auto *segInfo = handshake.add_seg_infos();
        auto *seg = segInfo->mutable_seg();
        seg->set_eid(kFakeEid);
        seg->set_uasid(1);
        seg->set_va(kRemoteSegVa);
        seg->set_len(64 * kMb);

        UrmaJfrInfo peerInfo;
        ASSERT_TRUE(peerInfo.FromProto(handshake).IsOk());
        uint32_t localJettyId = 0;
        ASSERT_TRUE(manager.ImportRemoteJetty(peerInfo, localJettyId).IsOk());
        ASSERT_TRUE(manager.ImportRemoteInfo(handshake).IsOk());
    }

    static bool IsMockBackendAvailable()
    {
#ifdef USE_URMA_MOCK
        return true;
#else
        return false;
#endif
    }

    // Posts dataSize bytes through the production write path, collects one (chunkIndex,
    // chunkCount, chunkSize) triple per event, then waits for every post to finish.
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> WriteAndCollectChunks(uint64_t dataSize, Status &writeRc)
    {
        auto &manager = UrmaManager::Instance();
        const uint64_t bufferRoundUp = ((dataSize + kMb - 1) / kMb) * kMb;
        MmapBuffer payload(bufferRoundUp);
        if (!payload.IsValid()) {
            writeRc = Status(K_OUT_OF_MEMORY, "Failed to allocate URMA test payload");
            return {};
        }
        UrmaRemoteAddrPb remote;
        remote.set_seg_va(kRemoteSegVa);
        remote.set_seg_data_offset(0);
        remote.mutable_request_address()->set_host(kRemoteAddress);
        remote.mutable_request_address()->set_port(kRemotePort);
        std::vector<uint64_t> eventKeys;
        writeRc = manager.UrmaWritePayload(remote, reinterpret_cast<uint64_t>(payload.Data()), payload.Size(),
                                           reinterpret_cast<uint64_t>(payload.Data()), 0, dataSize, 0, INVALID_CHIP_ID,
                                           INVALID_CHIP_ID, false, eventKeys);
        if (writeRc.IsError()) {
            return {};
        }
        std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> chunks;
        chunks.reserve(eventKeys.size());
        bool inspectedAllEvents = true;
        for (const auto key : eventKeys) {
            std::shared_ptr<UrmaEvent> event;
            if (!manager.GetEvent(key, event).IsOk()) {
                ADD_FAILURE() << "event " << key << " vanished before chunk inspection";
                inspectedAllEvents = false;
                continue;
            }
            const auto trace = event->GetWriteTrace();
            chunks.emplace_back(trace.writeChunkIndex, trace.writeChunkCount, event->GetDataSize());
        }
        Status firstWaitError = Status::OK();
        for (const auto key : eventKeys) {
            const auto waitRc = manager.WaitToFinish(key, kWriteWaitTimeoutMs);
            if (waitRc.IsError() && firstWaitError.IsOk()) {
                firstWaitError = waitRc;
            }
        }
        if (firstWaitError.IsError()) {
            writeRc = firstWaitError;
            return {};
        }
        return inspectedAllEvents ? chunks : std::vector<std::tuple<uint64_t, uint64_t, uint64_t>>{};
    }

    static void ExpectEvenChunks(const std::vector<std::tuple<uint64_t, uint64_t, uint64_t>> &chunks,
                                 uint64_t dataSize, uint64_t maxWriteSize, const char *context)
    {
        ASSERT_FALSE(chunks.empty()) << context;
        const uint64_t expectedCount = (dataSize + maxWriteSize - 1) / maxWriteSize;
        ASSERT_EQ(chunks.size(), expectedCount) << context << ": chunk count is not minimal";
        // Even split within the minimal count: every non-final chunk is ceil(dataSize/count),
        // which never exceeds maxWriteSize; the final chunk carries the remainder.
        const uint64_t expectedChunkSize = (dataSize + expectedCount - 1) / expectedCount;
        uint64_t summedSize = 0;
        uint64_t smallestChunk = std::numeric_limits<uint64_t>::max();
        uint64_t largestChunk = 0;
        for (size_t i = 0; i < chunks.size(); ++i) {
            const auto &[chunkIndex, chunkCount, chunkSize] = chunks[i];
            EXPECT_EQ(chunkIndex, i + 1) << context << ": chunk index out of order";
            EXPECT_EQ(chunkCount, expectedCount) << context << ": chunk count mismatch across events";
            EXPECT_LE(chunkSize, maxWriteSize) << context << ": chunk exceeds the configured max write size";
            if (i + 1 < chunks.size()) {
                EXPECT_EQ(chunkSize, expectedChunkSize) << context << ": non-final chunk is not evenly split";
            }
            summedSize += chunkSize;
            smallestChunk = std::min(smallestChunk, chunkSize);
            largestChunk = std::max(largestChunk, chunkSize);
        }
        EXPECT_EQ(summedSize, dataSize) << context << ": chunks do not cover the payload exactly";
        EXPECT_LE(largestChunk - smallestChunk, 1u) << context << ": chunks differ by more than one byte";
        EXPECT_EQ(std::get<2>(chunks.back()), dataSize - expectedChunkSize * (expectedCount - 1))
            << context << ": final chunk does not carry the remainder";
    }

    uint64_t maxWriteSize_ = 0;
    RaiiPlus flagRestore_;

private:
    // Page-aligned anonymous buffer large enough for the payload; registered as the local segment.
    class MmapBuffer {
    public:
        explicit MmapBuffer(uint64_t size) : size_(size)
        {
            data_ = static_cast<uint8_t *>(mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
                                                -1, 0));
            if (data_ == MAP_FAILED) {
                data_ = nullptr;
            }
        }

        ~MmapBuffer()
        {
            if (data_ != nullptr) {
                (void)munmap(data_, size_);
            }
        }

        MmapBuffer(const MmapBuffer &) = delete;
        MmapBuffer &operator=(const MmapBuffer &) = delete;

        bool IsValid() const
        {
            return data_ != nullptr;
        }

        uint8_t *Data() const
        {
            return data_;
        }

        uint64_t Size() const
        {
            return size_;
        }

    private:
        uint8_t *data_ = nullptr;
        uint64_t size_ = 0;
    };
};

TEST_F(UrmaWriteChunkSplitTest, SplitUsesMinimumCountAndBalancesChunks)
{
    const std::vector<std::pair<uint64_t, const char *>> cases = {
        { maxWriteSize_, "limit payload" },
        { maxWriteSize_ + 1, "limit plus one byte" },
        { 2 * maxWriteSize_ + 3 * kMb / 4, "three uneven chunks" },
    };
    for (const auto &[dataSize, context] : cases) {
        Status writeRc;
        const auto chunks = WriteAndCollectChunks(dataSize, writeRc);
        ASSERT_TRUE(writeRc.IsOk()) << context << ": " << writeRc.ToString();
        ExpectEvenChunks(chunks, dataSize, maxWriteSize_, context);
    }
}

}  // namespace
}  // namespace datasystem

#else
TEST(UrmaWriteChunkSplitTest, RequiresUrmaBuildConfiguration)
{
    GTEST_SKIP() << "Build this target with --config=urma_mock.";
}
#endif
