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
package org.apache.asterix.external.parser.factory;

import java.util.List;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.api.IRecordDataParser;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.IcebergParquetDataParser;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.iceberg.data.Record;

public class IcebergTableParquetDataParserFactory extends IcebergParserFactory<Record> {

    private static final long serialVersionUID = 1L;
    private static final List<String> PARSER_FORMAT = List.of(IcebergConstants.ICEBERG_PARQUET_FORMAT);

    @Override
    public IStreamDataParser createInputStreamParser(IExternalDataRuntimeContext context) {
        throw new UnsupportedOperationException("Stream parser is not supported");
    }

    @Override
    public void setMetaType(ARecordType metaType) {
        // no MetaType to set.
    }

    @Override
    public List<String> getParserFormats() {
        return PARSER_FORMAT;
    }

    @Override
    public IRecordDataParser<Record> createRecordParser(IExternalDataRuntimeContext context)
            throws HyracksDataException {
        return createParser(context);
    }

    @Override
    public Class<?> getRecordClass() {
        return Record.class;
    }

    /**
     * @throws HyracksDataException if the projected schema has a column the parser cannot read, the same check the
     *                              reader factory makes at compile time
     */
    private IcebergParquetDataParser createParser(IExternalDataRuntimeContext context) throws HyracksDataException {
        IApplicationContext appCtx = (IApplicationContext) context.getTaskContext().getJobletContext()
                .getServiceContext().getApplicationContext();
        Integer id = IcebergParquetDataParser.findUnsupportedColumn(projectedSchema,
                appCtx.getExternalProperties().isIcebergVariantEnabled());
        if (id != null) {
            throw RuntimeDataException.create(ErrorCode.UNSUPPORTED_ICEBERG_TYPE,
                    projectedSchema.findType(id).toString(), projectedSchema.findColumnName(id));
        }
        return new IcebergParquetDataParser(context, configuration, projectedSchema);
    }
}
