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

#include <cstdint>
#include <limits>

#include "lib/Commands.h"
#include "lib/ConsumerImpl.h"

using namespace pulsar;

// The layout of a chunked message comes from the metadata written by the producer, and the consumer
// allocates the bookkeeping for it before receiving the rest of the chunks. These cases are rejected
// without allocating anything.
TEST(ChunkedMessageLayoutTest, testInvalidLayouts) {
    const int64_t maxMessageSize = Commands::DefaultMaxMessageSize;
    const int64_t maxInt = std::numeric_limits<int32_t>::max();

    // A tiny payload declaring a huge number of chunks.
    ASSERT_FALSE(ConsumerImpl::isValidChunkedMessageLayout(maxInt, 1, maxMessageSize));
    ASSERT_FALSE(ConsumerImpl::isValidChunkedMessageLayout(3, 2, maxMessageSize));
    // Non-positive values.
    ASSERT_FALSE(ConsumerImpl::isValidChunkedMessageLayout(0, 1, maxMessageSize));
    ASSERT_FALSE(ConsumerImpl::isValidChunkedMessageLayout(-1, 1, maxMessageSize));
    ASSERT_FALSE(ConsumerImpl::isValidChunkedMessageLayout(2, 0, maxMessageSize));
    ASSERT_FALSE(ConsumerImpl::isValidChunkedMessageLayout(2, -1, maxMessageSize));
    // Chunks bigger than the maximum message size.
    ASSERT_FALSE(ConsumerImpl::isValidChunkedMessageLayout(2, 2 * maxMessageSize + 1, maxMessageSize));
    // A total size above the limit of an assembled message.
    ASSERT_FALSE(ConsumerImpl::isValidChunkedMessageLayout(maxInt, maxInt, maxMessageSize));
}

TEST(ChunkedMessageLayoutTest, testValidLayouts) {
    const int64_t maxMessageSize = Commands::DefaultMaxMessageSize;
    ASSERT_TRUE(ConsumerImpl::isValidChunkedMessageLayout(2, 2, maxMessageSize));
    ASSERT_TRUE(ConsumerImpl::isValidChunkedMessageLayout(2, maxMessageSize + 1, maxMessageSize));
    ASSERT_TRUE(ConsumerImpl::isValidChunkedMessageLayout(3, 3 * maxMessageSize, maxMessageSize));
}
