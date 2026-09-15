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
package org.apache.asterix.external.library.msgpack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.DataOutput;

import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

/**
 * Strings handed to a Python UDF must be packed as standard UTF-8, not as the modified UTF-8 they are stored in.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Regression test for supplementary characters mis-sized in msgpack STR headers")
public class MsgPackStringAccessorTest {

    private static final String EMOJI = "😀";

    @Test
    public void testAscii() throws Exception {
        assertPacksAs("hello");
    }

    @Test
    public void testTwoByteChars() throws Exception {
        assertPacksAs("café über");
    }

    @Test
    public void testThreeByteChars() throws Exception {
        assertPacksAs("你好世界");
    }

    @Test
    public void testSupplementaryChars() throws Exception {
        assertPacksAs(EMOJI);
        assertPacksAs("a" + EMOJI + "b");
        assertPacksAs(EMOJI + EMOJI + EMOJI);
        assertPacksAs("mixed é 你 " + EMOJI + " tail");
    }

    /**
     * A STR header that overstates its payload does not corrupt only that string: the unpacker keeps reading into
     * whatever follows it in the message.
     */
    @Test
    public void testSupplementaryCharsDoNotDesyncFollowingValues() throws Exception {
        ArrayBackedValueStorage packed = new ArrayBackedValueStorage();
        pack("a" + EMOJI, packed);
        pack("b" + EMOJI, packed);
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packed.getByteArray(), 0, packed.getLength())) {
            assertEquals("a" + EMOJI, unpacker.unpackString());
            assertEquals("b" + EMOJI, unpacker.unpackString());
            assertFalse(unpacker.hasNext());
        }
    }

    /**
     * Malformed input still has to be framed correctly: a STR header disagreeing with its payload would leave the
     * rest of the message unreadable, which is a worse failure than the mojibake the character itself becomes.
     */
    @Test
    public void testUnpairedHighSurrogateIsStillFramedCorrectly() throws Exception {
        ArrayBackedValueStorage packed = new ArrayBackedValueStorage();
        pack("\uD800a", packed);
        pack("tail", packed);
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packed.getByteArray(), 0, packed.getLength())) {
            unpacker.unpackString();
            assertEquals("tail", unpacker.unpackString());
            assertFalse(unpacker.hasNext());
        }
    }

    private static void assertPacksAs(String value) throws Exception {
        ArrayBackedValueStorage packed = new ArrayBackedValueStorage();
        pack(value, packed);
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(packed.getByteArray(), 0, packed.getLength())) {
            assertEquals(value, unpacker.unpackString());
            assertFalse(unpacker.hasNext());
        }
    }

    private static void pack(String value, ArrayBackedValueStorage out) throws Exception {
        ArrayBackedValueStorage serialized = new ArrayBackedValueStorage();
        DataOutput serializedOut = serialized.getDataOutput();
        serializedOut.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader()).serialize(new AString(value),
                serializedOut);
        VoidPointable pointable = new VoidPointable();
        pointable.set(serialized.getByteArray(), 0, serialized.getLength());
        MsgPackAccessors.createFlatMsgPackAccessor(ATypeTag.STRING).apply(pointable, out.getDataOutput());
    }
}
