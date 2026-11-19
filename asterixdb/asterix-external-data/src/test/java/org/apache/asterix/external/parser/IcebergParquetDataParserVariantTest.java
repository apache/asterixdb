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
package org.apache.asterix.external.parser;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.input.filter.NoOpFilterValueEmbedder;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.parser.factory.IcebergTableParquetDataParserFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.Variants;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class IcebergParquetDataParserVariantTest {

    private static final Variant VALUE = Variant.of(Variants.emptyMetadata(), Variants.of(42));
    private static final Schema TOP_LEVEL =
            new Schema(required(1, "id", Types.IntegerType.get()), optional(2, "v", Types.VariantType.get()));
    private static final Schema IN_LIST = new Schema(required(1, "id", Types.IntegerType.get()),
            optional(2, "vs", Types.ListType.ofOptional(3, Types.VariantType.get())));

    @Test
    public void parsesVariant() throws HyracksDataException {
        Assert.assertTrue(parse(TOP_LEVEL, record(TOP_LEVEL, "v", VALUE)));
        Assert.assertTrue(parse(IN_LIST, record(IN_LIST, "vs", List.of(VALUE))));
    }

    @Test
    public void parsesNullVariant() throws HyracksDataException {
        Assert.assertTrue(parse(TOP_LEVEL, record(TOP_LEVEL, "v", null)));
    }

    @Test
    public void parsesColumnsWithoutVariant() throws HyracksDataException {
        Schema idOnly = TOP_LEVEL.select("id");
        Assert.assertTrue(parse(idOnly, record(idOnly, null, null)));
    }

    @Test
    public void geometryRejected() {
        Schema geo = new Schema(required(1, "id", Types.IntegerType.get()),
                optional(2, "shape", Types.GeometryType.crs84()));
        try {
            parse(geo, record(geo, "shape", ByteBuffer.wrap(new byte[] { 1 })));
            Assert.fail("expected GEOMETRY to be rejected");
        } catch (HyracksDataException e) {
            Assert.assertEquals(ErrorCode.UNSUPPORTED_ICEBERG_TYPE.intValue(), e.getErrorCode());
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("for column 'shape'"));
        }
    }

    /** An UNKNOWN column holds only nulls, so it still parses. */
    @Test
    public void unknownColumnParses() throws HyracksDataException {
        Schema unknown =
                new Schema(required(1, "id", Types.IntegerType.get()), optional(2, "nothing", Types.UnknownType.get()));
        Assert.assertTrue(parse(unknown, record(unknown, "nothing", null)));
    }

    private static boolean parse(Schema schema, Record record) throws HyracksDataException {
        IcebergTableParquetDataParserFactory factory = new IcebergTableParquetDataParserFactory();
        factory.configure(Collections.emptyMap());
        factory.setProjectedSchema(schema);
        IRecordDataParser<Record> parser = factory.createRecordParser(runtimeContext());
        return parser.parse(new GenericRecord<>(record), new ArrayBackedValueStorage().getDataOutput());
    }

    private static Record record(Schema schema, String field, Object value) {
        Record record = org.apache.iceberg.data.GenericRecord.create(schema);
        record.setField("id", 1);
        if (field != null) {
            record.setField(field, value);
        }
        return record;
    }

    private static IExternalDataRuntimeContext runtimeContext() {
        IHyracksTaskContext taskCtx = Mockito.mock(IHyracksTaskContext.class);
        Mockito.when(taskCtx.getWarningCollector()).thenReturn(Mockito.mock(IWarningCollector.class));
        IExternalDataRuntimeContext context = Mockito.mock(IExternalDataRuntimeContext.class);
        Mockito.when(context.getTaskContext()).thenReturn(taskCtx);
        Mockito.when(context.getValueEmbedder()).thenReturn(NoOpFilterValueEmbedder.INSTANCE);
        return context;
    }
}
