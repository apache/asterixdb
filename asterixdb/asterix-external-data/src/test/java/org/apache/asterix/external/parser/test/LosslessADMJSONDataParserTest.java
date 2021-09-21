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

package org.apache.asterix.external.parser.test;

import static org.apache.asterix.om.types.AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.dataflow.data.nontagged.serde.AObjectSerializerDeserializer;
import org.apache.asterix.external.parser.LosslessADMJSONDataParser;
import org.apache.asterix.formats.nontagged.LosslessADMJSONPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ACircle;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADayTimeDuration;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.ALine;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.AMutableUUID;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.APoint;
import org.apache.asterix.om.base.APoint3D;
import org.apache.asterix.om.base.APolygon;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.ATime;
import org.apache.asterix.om.base.AUUID;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.AYearMonthDuration;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.fasterxml.jackson.core.JsonFactory;

@RunWith(Parameterized.class)
public class LosslessADMJSONDataParserTest {

    static final IAObject[] PRIMITIVE_VALUES = new IAObject[] {
            //
            AMissing.MISSING,
            //
            ANull.NULL,
            //
            ABoolean.TRUE,
            //
            ABoolean.FALSE,
            //
            new AInt8((byte) 8),
            //
            new AInt8((byte) -8),
            //
            new AInt16((short) 16),
            //
            new AInt16((short) -16),
            //
            new AInt32((short) 32),
            //
            new AInt32((short) -32),
            //
            new AInt64(64),
            //
            new AInt64(-64),
            //
            new AFloat(1.5f),
            //
            new AFloat(-1.5f),
            //
            new AFloat(Float.POSITIVE_INFINITY),
            //
            new AFloat(Float.NEGATIVE_INFINITY),
            //
            new AFloat(Float.NaN),
            //
            new ADouble(1.5d),
            //
            new ADouble(-1.5d),
            //
            new ADouble(Double.POSITIVE_INFINITY),
            //
            new ADouble(Double.NEGATIVE_INFINITY),
            //
            new ADouble(Double.NaN),
            //
            new AString(""),
            //
            new AString(" "),
            //
            new AString(":"),
            //
            new AString("hello"),
            //
            new ADate(2),
            //
            new ATime((int) TimeUnit.HOURS.toMillis(3)),
            //
            new ADateTime(TimeUnit.DAYS.toMillis(4)),
            //
            new AYearMonthDuration(2),
            //
            new ADayTimeDuration((int) TimeUnit.HOURS.toMillis(3)),
            //
            new ADuration(4, (int) TimeUnit.HOURS.toMillis(5)),
            //
            new ABinary(new byte[] {}),
            //
            new ABinary(new byte[] { 1, 2, 3, 4 }),
            //
            createUUID(UUID.randomUUID()),
            //
            new APoint(1.5, -2.5),
            //
            new APoint3D(-1.5, 2.5, 3.5),
            //
            new ACircle(new APoint(1.5, -2.5), 3.5),
            //
            new ALine(new APoint(-1.5, -2.5), new APoint(3.5, 4.5)),
            //
            new ARectangle(new APoint(-1.5, -2.5), new APoint(3.5, 4.5)),
            //
            new APolygon(new APoint[] { new APoint(-1.5, -2.5), new APoint(-1.5, 2.5), new APoint(1.5, 2.5),
                    new APoint(1.5, -2.5) }) };
    private final String label;
    private final IAObject inValue;
    private final IAObject expectedOutValue;

    public LosslessADMJSONDataParserTest(String label, IAObject inValue, IAObject expectedOutValue) {
        this.label = label;
        this.inValue = Objects.requireNonNull(inValue);
        this.expectedOutValue = expectedOutValue == null ? inValue : expectedOutValue;
    }

    @Parameterized.Parameters(name = "LosslessADMJSONDataParserTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        List<Object[]> tests = new ArrayList<>();
        testsForPrimitives(tests);
        testsForArrays(tests);
        testsForObjects(tests);
        return tests;
    }

    private static void testsForPrimitives(List<Object[]> outTests) {
        for (IAObject v : PRIMITIVE_VALUES) {
            outTests.add(testcase(v));
        }
    }

    private static void testsForArrays(List<Object[]> outTests) {
        for (IAObject v1 : PRIMITIVE_VALUES) {
            for (IAObject v2 : PRIMITIVE_VALUES) {
                // array of primitives
                AOrderedList ol1 = new AOrderedList(FULL_OPEN_ORDEREDLIST_TYPE, Arrays.asList(v1, v2));
                outTests.add(testcase(ol1));
                // multiset of primitives (printed as array)
                AUnorderedList ul1 =
                        new AUnorderedList(AUnorderedListType.FULLY_OPEN_UNORDEREDLIST_TYPE, Arrays.asList(v1, v2));
                outTests.add(testcase(ul1, ol1));
                // array of arrays
                AOrderedList ol2 = new AOrderedList(FULL_OPEN_ORDEREDLIST_TYPE, Arrays.asList(v2, v1));
                AOrderedList ol3 = new AOrderedList(FULL_OPEN_ORDEREDLIST_TYPE, Arrays.asList(ol1, ol2));
                outTests.add(testcase(ol3));
            }
        }
    }

    private static void testsForObjects(List<Object[]> outTests) {
        // flat
        List<String> fieldNames = new ArrayList<>();
        List<IAType> fieldTypes = new ArrayList<>();
        List<IAObject> fieldValues = new ArrayList<>();
        for (IAObject v : PRIMITIVE_VALUES) {
            if (v.getType().getTypeTag() != ATypeTag.BIGINT) {
                continue;
            }
            fieldNames.add("f" + fieldNames.size());
            fieldTypes.add(v.getType());
            fieldValues.add(v);
        }
        ARecordType rt0 =
                new ARecordType("rt0", fieldNames.toArray(new String[0]), fieldTypes.toArray(new IAType[0]), true);
        ARecord r0 = new ARecord(rt0, fieldValues.toArray(new IAObject[0]));
        outTests.add(testcase(r0));

        // nested
        ARecordType rt1 = new ARecordType("rt1", new String[] { "n1", "n2" }, new IAType[] { rt0, rt0 }, true);
        ARecord r1 = new ARecord(rt1, new IAObject[] { r0, r0 });
        outTests.add(testcase(r1));
    }

    private static Object[] testcase(IAObject v) {
        return testcase(v, v);
    }

    private static Object[] testcase(IAObject v, IAObject expectedOutValue) {
        return new Object[] { String.format("%s(%s)", v.getType().getTypeName(), v), v, expectedOutValue };
    }

    private static AUUID createUUID(UUID uuid) {
        try {
            AMutableUUID m = new AMutableUUID();
            char[] text = uuid.toString().toCharArray();
            m.parseUUIDString(text, 0, text.length);
            return m;
        } catch (HyracksDataException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test() throws Exception {
        ByteArrayAccessibleOutputStream baosInSer = new ByteArrayAccessibleOutputStream();
        ISerializerDeserializer serde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(inValue.getType());
        serde.serialize(inValue, new DataOutputStream(baosInSer));

        ByteArrayAccessibleOutputStream baosPrint = new ByteArrayAccessibleOutputStream();
        IPrinter printer =
                LosslessADMJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(inValue.getType()).createPrinter();
        printer.print(baosInSer.getByteArray(), 0, baosInSer.getLength(),
                new PrintStream(baosPrint, true, StandardCharsets.UTF_8));

        ByteArrayAccessibleOutputStream baosParse = new ByteArrayAccessibleOutputStream();
        LosslessADMJSONDataParser lp = new LosslessADMJSONDataParser(new JsonFactory());
        lp.setInputStream(new ByteArrayAccessibleInputStream(baosPrint.getByteArray(), 0, baosPrint.getLength()));
        lp.parseAnyValue(new DataOutputStream(baosParse));

        IAObject outValue = AObjectSerializerDeserializer.INSTANCE.deserialize(new DataInputStream(
                new ByteArrayAccessibleInputStream(baosParse.getByteArray(), 0, baosParse.getLength())));

        if (!expectedOutValue.deepEqual(outValue)) {
            Assert.fail(String.format(
                    "%s print/parse test failed. In value: %s, Expected out value: %s, Actual out value: %s, Encoded value: %s",
                    label, inValue, expectedOutValue, outValue,
                    new String(baosPrint.getByteArray(), 0, baosPrint.getLength(), StandardCharsets.UTF_8)));
        }
    }
}
