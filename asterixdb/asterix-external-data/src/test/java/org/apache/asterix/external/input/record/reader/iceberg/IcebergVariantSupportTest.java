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
package org.apache.asterix.external.input.record.reader.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.asterix.runtime.projection.ExternalDatasetProjectionFiltrationInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class IcebergVariantSupportTest {

    private static final Schema SCHEMA =
            new Schema(required(1, "id", Types.IntegerType.get()), optional(2, "v", Types.VariantType.get()),
                    optional(3, "st",
                            Types.StructType.of(optional(4, "sv", Types.VariantType.get()),
                                    optional(5, "label", Types.StringType.get()))),
                    optional(6, "vs", Types.ListType.ofOptional(7, Types.VariantType.get())), optional(8, "vm",
                            Types.MapType.ofOptional(9, 10, Types.StringType.get(), Types.VariantType.get())));

    @Test
    public void disabledRejectsVariantAtAnyDepth() {
        Map<String, String> columns = Map.of("v", "v", "st", "st.sv", "vs", "vs.element", "vm", "vm.value");
        columns.forEach((selected, expected) -> {
            try {
                IcebergParquetRecordReaderFactory.ensureTypesSupported(SCHEMA.select(selected), false);
                Assert.fail("expected " + selected + " to be rejected");
            } catch (CompilationException e) {
                Assert.assertEquals(selected, ErrorCode.UNSUPPORTED_ICEBERG_TYPE.intValue(),
                        e.getError().orElseThrow().intValue());
                Assert.assertTrue(e.getMessage(),
                        e.getMessage().contains("Unsupported Iceberg type 'variant' for column '" + expected + "'"));
            }
        });
    }

    @Test
    public void disabledAllowsColumnsWithoutVariant() throws CompilationException {
        IcebergParquetRecordReaderFactory.ensureTypesSupported(SCHEMA.select("id"), false);
        IcebergParquetRecordReaderFactory.ensureTypesSupported(new Schema(), false);
    }

    @Test
    public void enabledAllowsVariant() throws CompilationException {
        IcebergParquetRecordReaderFactory.ensureTypesSupported(SCHEMA, true);
    }

    /** GEOMETRY and GEOGRAPHY cannot be read whatever the VARIANT setting, and the column is named for excluding. */
    @Test
    public void geospatialRejectedAtAnyDepth() {
        Schema geo =
                new Schema(required(1, "id", Types.IntegerType.get()), optional(2, "shape", Types.GeometryType.crs84()),
                        optional(3, "st", Types.StructType.of(optional(4, "area", Types.GeographyType.crs84()))));
        Map<String, String> columns = Map.of("shape", "for column 'shape'", "st", "for column 'st.area'");
        for (boolean variantEnabled : new boolean[] { true, false }) {
            columns.forEach((selected, expected) -> {
                try {
                    IcebergParquetRecordReaderFactory.ensureTypesSupported(geo.select(selected), variantEnabled);
                    Assert.fail("expected " + selected + " to be rejected");
                } catch (CompilationException e) {
                    Assert.assertEquals(selected, ErrorCode.UNSUPPORTED_ICEBERG_TYPE.intValue(),
                            e.getError().orElseThrow().intValue());
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains(expected));
                }
            });
        }
    }

    /** An UNKNOWN column holds only nulls, so it can be read. */
    @Test
    public void unknownAllowed() throws CompilationException {
        Schema unknown = new Schema(optional(1, "nothing", Types.UnknownType.get()));
        IcebergParquetRecordReaderFactory.ensureTypesSupported(unknown, false);
    }

    @Test
    public void noRequestedFieldsSelectsNoColumns() throws Exception {
        Schema selected = IcebergUtils.selectRequestedColumns(SCHEMA,
                requested(ProjectionFiltrationTypeUtil.EMPTY_TYPE.getTypeName()));
        Assert.assertTrue(selected.columns().isEmpty());
    }

    @Test
    public void allRequestedFieldsSelectsEveryColumn() throws Exception {
        Schema selected = IcebergUtils.selectRequestedColumns(SCHEMA,
                requested(ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE.getTypeName()));
        Assert.assertEquals(SCHEMA.asStruct(), selected.asStruct());
    }

    @Test
    public void someRequestedFieldsSelectsThoseColumns() throws Exception {
        ARecordType type = ProjectionFiltrationTypeUtil.getRecordType(List.of(List.of("id")));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ExternalDatasetProjectionFiltrationInfo.writeTypeField(type, new DataOutputStream(bytes));
        Schema selected = IcebergUtils.selectRequestedColumns(SCHEMA,
                requested(Base64.getEncoder().encodeToString(bytes.toByteArray())));
        Assert.assertEquals(SCHEMA.select("id").asStruct(), selected.asStruct());
    }

    private static Map<String, String> requested(String encoded) {
        return Map.of(ExternalDataConstants.KEY_REQUESTED_FIELDS, encoded);
    }
}
