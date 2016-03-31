/*
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
package org.apache.asterix.external.input.record.converter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.record.RecordWithMetadataAndPK;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;

public class DCPRequestToRecordWithMetadataAndPKConverter
        implements IRecordToRecordWithMetadataAndPKConverter<DCPRequest, char[]> {

    private final RecordWithMetadataAndPK<char[]> recordWithMetadata;
    private final CharArrayRecord value;
    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private final ByteBuffer bytes = ByteBuffer.allocateDirect(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    private final CharBuffer chars = CharBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    // metaTypes = {key(string), bucket(string), vbucket(int32), seq(long), cas(long),
    // creationTime(long),expiration(int32),flags(int32),revSeqNumber(long),lockTime(int32)}
    private static final IAType[] CB_META_TYPES = new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING,
            BuiltinType.AINT32, BuiltinType.AINT64, BuiltinType.AINT64, BuiltinType.AINT64, BuiltinType.AINT32,
            BuiltinType.AINT32, BuiltinType.AINT64, BuiltinType.AINT32 };
    private static final int[] PK_INDICATOR = { 1 };
    private static final int[] PK_INDEXES = { 0 };
    private static final IAType[] PK_TYPES = { BuiltinType.ASTRING };

    public DCPRequestToRecordWithMetadataAndPKConverter() {
        this.value = new CharArrayRecord();
        this.recordWithMetadata = new RecordWithMetadataAndPK<char[]>(value, CB_META_TYPES,
                ARecordType.FULLY_OPEN_RECORD_TYPE, PK_INDICATOR, PK_INDEXES, PK_TYPES);
    }

    @Override
    public RecordWithMetadataAndPK<char[]> convert(final IRawRecord<? extends DCPRequest> input) throws IOException {
        final DCPRequest dcpRequest = input.get();
        if (dcpRequest instanceof MutationMessage) {
            final MutationMessage message = (MutationMessage) dcpRequest;
            final String key = message.key();
            final int vbucket = message.partition();
            final long seq = message.bySequenceNumber();
            final String bucket = message.bucket();
            final long cas = message.cas();
            final long creationTime = message.creationTime();
            final int expiration = message.expiration();
            final int flags = message.flags();
            final long revSeqNumber = message.revisionSequenceNumber();
            final int lockTime = message.lockTime();
            recordWithMetadata.reset();
            recordWithMetadata.setMetadata(0, key);
            recordWithMetadata.setMetadata(1, bucket);
            recordWithMetadata.setMetadata(2, vbucket);
            recordWithMetadata.setMetadata(3, seq);
            recordWithMetadata.setMetadata(4, cas);
            recordWithMetadata.setMetadata(5, creationTime);
            recordWithMetadata.setMetadata(6, expiration);
            recordWithMetadata.setMetadata(7, flags);
            recordWithMetadata.setMetadata(8, revSeqNumber);
            recordWithMetadata.setMetadata(9, lockTime);
            DCPRequestToRecordWithMetadataAndPKConverter.set(message.content(), decoder, bytes, chars, value);
        } else if (dcpRequest instanceof RemoveMessage) {
            final RemoveMessage message = (RemoveMessage) dcpRequest;
            final String key = message.key();
            recordWithMetadata.reset();
            recordWithMetadata.setMetadata(0, key);
        } else {
            throw new HyracksDataException("Unknown DCP request: " + dcpRequest);
        }
        return recordWithMetadata;
    }

    public static void set(final ByteBuf content, final CharsetDecoder decoder, final ByteBuffer bytes,
            final CharBuffer chars, final CharArrayRecord record) throws IOException {
        int position = content.readerIndex();
        final int limit = content.writerIndex();
        final int contentSize = content.readableBytes();
        while (position < limit) {
            bytes.clear();
            chars.clear();
            if ((contentSize - position) < bytes.capacity()) {
                bytes.limit(contentSize - position);
            }
            content.getBytes(position, bytes);
            position += bytes.position();
            bytes.flip();
            decoder.decode(bytes, chars, false);
            chars.flip();
            record.append(chars);
        }
        record.endRecord();
    }
}
