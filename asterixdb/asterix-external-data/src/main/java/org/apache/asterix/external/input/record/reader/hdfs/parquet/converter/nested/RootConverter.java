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
package org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.nested;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.hdfs.parquet.converter.ParquetConverterContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.parquet.schema.GroupType;

public class RootConverter extends ObjectConverter {
    private final ArrayBackedValueStorage rootBuffer;

    public RootConverter(GroupType parquetType, IExternalFilterValueEmbedder valueEmbedder, Configuration configuration,
            List<Warning> warnings) throws IOException {
        super(null, -1, parquetType, new ParquetConverterContext(configuration, valueEmbedder, warnings));
        this.rootBuffer = new ArrayBackedValueStorage();
    }

    @Override
    protected DataOutput getParentDataOutput() {
        rootBuffer.reset();
        return rootBuffer.getDataOutput();
    }

    @Override
    protected boolean isRoot() {
        return true;
    }

    public IValueReference getRecord() {
        return rootBuffer;
    }

}
