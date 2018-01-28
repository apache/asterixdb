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

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.asterix.external.input.record.RecordWithMetadataAndPK;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.RecordUtil;

import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.util.ReferenceCountUtil;

public class DCPMessageToRecordConverter implements IRecordToRecordWithMetadataAndPKConverter<DCPRequest, char[]> {

    private final RecordWithMetadataAndPK<char[]> recordWithMetadata;
    private final CharArrayRecord value;
    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private final ByteBuffer bytes = ByteBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    private final CharBuffer chars = CharBuffer.allocate(ExternalDataConstants.DEFAULT_BUFFER_SIZE);
    private static final IAType[] CB_META_TYPES = new IAType[] { /*ID*/BuiltinType.ASTRING, /*VBID*/BuiltinType.AINT32,
            /*SEQ*/BuiltinType.AINT64, /*CAS*/BuiltinType.AINT64, /*EXPIRATION*/BuiltinType.AINT32,
            /*FLAGS*/BuiltinType.AINT32, /*REV*/BuiltinType.AINT64, /*LOCK*/BuiltinType.AINT32 };
    private static final int[] PK_INDICATOR = { 1 };
    private static final int[] PK_INDEXES = { 0 };
    private static final IAType[] PK_TYPES = { BuiltinType.ASTRING };

    public DCPMessageToRecordConverter() {
        this.value = new CharArrayRecord();
        this.recordWithMetadata = new RecordWithMetadataAndPK<>(value, CB_META_TYPES, RecordUtil.FULLY_OPEN_RECORD_TYPE,
                PK_INDICATOR, PK_INDEXES, PK_TYPES);
    }

    @Override
    public RecordWithMetadataAndPK<char[]> convert(final IRawRecord<? extends DCPRequest> input) throws IOException {
        final DCPRequest dcpRequest = input.get();
        if (dcpRequest instanceof MutationMessage) {
            final MutationMessage message = (MutationMessage) dcpRequest;
            try {
                final String key = message.key();
                final int vbucket = message.partition();
                final long seq = message.bySequenceNumber();
                final long cas = message.cas();
                final int expiration = message.expiration();
                final int flags = message.flags();
                final long revSeqNumber = message.revisionSequenceNumber();
                final int lockTime = message.lockTime();
                int i = 0;
                recordWithMetadata.reset();
                recordWithMetadata.setMetadata(i++, key);
                recordWithMetadata.setMetadata(i++, vbucket);
                recordWithMetadata.setMetadata(i++, seq);
                recordWithMetadata.setMetadata(i++, cas);
                recordWithMetadata.setMetadata(i++, expiration);
                recordWithMetadata.setMetadata(i++, flags);
                recordWithMetadata.setMetadata(i++, revSeqNumber);
                recordWithMetadata.setMetadata(i, lockTime);
                DCPMessageToRecordConverter.set(message.content(), decoder, bytes, chars, value);
            } finally {
                ReferenceCountUtil.release(message.content());
            }
        } else if (dcpRequest instanceof RemoveMessage) {
            final RemoveMessage message = (RemoveMessage) dcpRequest;
            final String key = message.key();
            recordWithMetadata.reset();
            recordWithMetadata.setMetadata(0, key);
        } else {
            throw new RuntimeDataException(
                    ErrorCode.INPUT_RECORD_CONVERTER_DCP_MSG_TO_RECORD_CONVERTER_UNKNOWN_DCP_REQUEST,
                    dcpRequest.toString());
        }
        return recordWithMetadata;
    }

    public static void set(final ByteBuf content, final CharsetDecoder decoder, final ByteBuffer bytes,
            final CharBuffer chars, final CharArrayRecord record) throws IOException {
        int position = content.readerIndex();
        final int limit = content.writerIndex();
        final int contentSize = content.readableBytes();
        bytes.clear();
        while (position < limit) {
            chars.clear();
            if ((contentSize - position) < bytes.capacity()) {
                bytes.limit(contentSize - position);
            }
            content.getBytes(position + bytes.position(), bytes);
            position += bytes.position();
            bytes.flip();
            decoder.decode(bytes, chars, false);
            if (bytes.hasRemaining()) {
                bytes.compact();
                position -= bytes.position();
            } else {
                bytes.clear();
            }
            chars.flip();
            record.append(chars);
        }
        record.endRecord();
    }
}
