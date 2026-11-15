/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <gtest/gtest.h>

#include <zlib.h>

#include "lib/CompressionCodecZLib.h"

using namespace pulsar;

TEST(ZLibCompressionTest, compressDecompress) {
    CompressionCodecZLib codec;

    std::string payload = "Payload to compress";
    SharedBuffer compressed = codec.encode(SharedBuffer::copy(payload.c_str(), payload.size()));

    SharedBuffer uncompressed;
    bool res = codec.decode(compressed, payload.size(), uncompressed);
    ASSERT_TRUE(res);
    ASSERT_EQ(payload, std::string(uncompressed.data(), uncompressed.readableBytes()));
}

// Java and C++ are using different ZLib settings when compressing, so the resulting
// compressed blobs are slightly different. Both should lead to the same result when
// decompressing
TEST(ZLibCompressionTest, decodeCppCompressed) {
    CompressionCodecZLib codec;

    const uint8_t compressed[] = {0x78, 0x9c, 0x63, 0x60, 0x80, 0x01, 0x00, 0x00, 0x0a, 0x00, 0x01};

    SharedBuffer uncompressed;
    uint32_t uncompressedSize = 10;

    bool res = codec.decode(SharedBuffer::copy((const char*)compressed, sizeof(compressed)), uncompressedSize,
                            uncompressed);
    ASSERT_TRUE(res);
    ASSERT_EQ(uncompressedSize, uncompressed.readableBytes());
}

TEST(ZLibCompressionTest, decodeJavaCompressed) {
    CompressionCodecZLib codec;

    const uint8_t compressed[] = {0x78, 0x9c, 0x62, 0x60, 0x80, 0x01, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff};

    SharedBuffer uncompressed;
    uint32_t uncompressedSize = 10;

    bool res = codec.decode(SharedBuffer::copy((const char*)compressed, sizeof(compressed)), uncompressedSize,
                            uncompressed);
    ASSERT_TRUE(res);
    ASSERT_EQ(uncompressedSize, uncompressed.readableBytes());
}

static SharedBuffer compressWithZlib(const std::string& payload, int flush) {
    z_stream stream{};
    EXPECT_EQ(Z_OK, deflateInit(&stream, Z_DEFAULT_COMPRESSION));
    std::string out(deflateBound(&stream, payload.size()) + 16, '\0');
    stream.next_in = (Bytef*)payload.data();
    stream.avail_in = payload.size();
    stream.next_out = (Bytef*)out.data();
    stream.avail_out = out.size();
    deflate(&stream, flush);
    out.resize(stream.total_out);
    deflateEnd(&stream);
    return SharedBuffer::copy(out.data(), out.size());
}

static bool decodeString(const SharedBuffer& compressed, uint32_t uncompressedSize, std::string& result) {
    CompressionCodecZLib codec;
    SharedBuffer uncompressed;
    if (!codec.decode(compressed, uncompressedSize, uncompressed)) {
        return false;
    }
    result.assign(uncompressed.data(), uncompressed.readableBytes());
    return true;
}

TEST(ZLibCompressionTest, rejectDeclaredSizeMismatch) {
    for (int flush : {Z_FINISH, Z_SYNC_FLUSH}) {
        SharedBuffer compressed = compressWithZlib("abc", flush);
        std::string result;
        ASSERT_TRUE(decodeString(compressed, 3, result));
        ASSERT_EQ("abc", result);
        ASSERT_FALSE(decodeString(compressed, 1, result));
        ASSERT_FALSE(decodeString(compressed, 2, result));
        ASSERT_FALSE(decodeString(compressed, 4, result));
    }
}

TEST(ZLibCompressionTest, rejectTruncatedInput) {
    for (int flush : {Z_FINISH, Z_SYNC_FLUSH}) {
        SharedBuffer compressed = compressWithZlib("abc", flush);
        std::string result;
        // A finished stream must not be accepted without its checksum trailer, and no
        // stream may be accepted with a part of its data missing. Without its final empty
        // stored block (the last 4 bytes), a sync-flushed stream still ends at a block
        // boundary and decodes to the complete data, so it is not checked.
        const uint32_t firstCut = flush == Z_SYNC_FLUSH ? 5 : 1;
        for (uint32_t cut = firstCut; cut < compressed.readableBytes(); ++cut) {
            SharedBuffer truncated = SharedBuffer::copy(compressed.data(), compressed.readableBytes() - cut);
            ASSERT_FALSE(decodeString(truncated, 3, result)) << "flush " << flush << ", cut " << cut;
        }
    }
}

TEST(ZLibCompressionTest, decodeEmpty) {
    for (int flush : {Z_FINISH, Z_SYNC_FLUSH}) {
        std::string result = "x";
        ASSERT_TRUE(decodeString(compressWithZlib("", flush), 0, result));
        ASSERT_EQ("", result);
    }
}
