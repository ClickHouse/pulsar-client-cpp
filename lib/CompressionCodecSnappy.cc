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
#include "CompressionCodecSnappy.h"

#if HAS_SNAPPY
#include <snappy-sinksource.h>
#include <snappy.h>

namespace pulsar {

SharedBuffer CompressionCodecSnappy::encode(const SharedBuffer& raw) {
    // Get the max size of the compressed data and allocate a buffer to hold it
    size_t maxCompressedLength = snappy::MaxCompressedLength(raw.readableBytes());
    SharedBuffer compressed = SharedBuffer::allocate(static_cast<const uint32_t>(maxCompressedLength));
    snappy::ByteArraySource source(raw.data(), raw.readableBytes());
    snappy::UncheckedByteArraySink sink(compressed.mutableData());
    size_t compressedSize = snappy::Compress(&source, &sink);
    compressed.setWriterIndex(static_cast<uint32_t>(compressedSize));
    return compressed;
}

bool CompressionCodecSnappy::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                    SharedBuffer& decoded) {
    // The length recorded inside the Snappy stream is independent from the one in the message metadata.
    // Both come from the network, so they must agree before decompressing into a buffer of
    // `uncompressedSize` bytes, otherwise a malformed message would overflow it.
    size_t streamUncompressedSize = 0;
    if (!snappy::GetUncompressedLength(encoded.data(), encoded.readableBytes(), &streamUncompressedSize) ||
        streamUncompressedSize != uncompressedSize) {
        return false;
    }

    SharedBuffer uncompressed = SharedBuffer::allocate(uncompressedSize);
    // `RawUncompress` never writes more than the length recorded in the stream, which is checked above.
    if (snappy::RawUncompress(encoded.data(), encoded.readableBytes(), uncompressed.mutableData())) {
        decoded = uncompressed;
        decoded.setWriterIndex(uncompressedSize);
        return true;
    } else {
        // Decompression failed
        return false;
    }
}
}  // namespace pulsar

#else  // No SNAPPY

#include <stdexcept>

namespace pulsar {

SharedBuffer CompressionCodecSnappy::encode(const SharedBuffer& raw) {
    throw std::runtime_error("Snappy compression not supported");
}

bool CompressionCodecSnappy::decode(const SharedBuffer& encoded, uint32_t uncompressedSize,
                                    SharedBuffer& decoded) {
    throw std::runtime_error("Snappy compression not supported");
}
}  // namespace pulsar

#endif  // HAS_SNAPPY
