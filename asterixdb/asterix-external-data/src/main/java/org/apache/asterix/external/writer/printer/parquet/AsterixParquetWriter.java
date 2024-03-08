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
package org.apache.asterix.external.writer.printer.parquet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.types.IAType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public class AsterixParquetWriter extends ParquetWriter<IValueReference> {
    public static Builder builder(Path file) {
        return new Builder(file);
    }

    public static Builder builder(OutputFile file) {
        return new Builder(file);
    }

    AsterixParquetWriter(Path file, WriteSupport<IValueReference> writeSupport,
            CompressionCodecName compressionCodecName, int blockSize, int pageSize, boolean enableDictionary,
            boolean enableValidation, ParquetProperties.WriterVersion writerVersion, Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize, pageSize, enableDictionary,
                enableValidation, writerVersion, conf);
    }

    public static class Builder extends ParquetWriter.Builder<IValueReference, Builder> {
        private MessageType type;
        private IAType typeInfo;
        private Map<String, String> extraMetaData;

        private Builder(Path file) {
            super(file);
            this.type = null;
            this.extraMetaData = new HashMap();
        }

        private Builder(OutputFile file) {
            super(file);
            this.type = null;
            this.extraMetaData = new HashMap();
        }

        public Builder withType(MessageType type) {
            this.type = type;
            return this;
        }

        public Builder withTypeInfo(IAType typeInfo) {
            this.typeInfo = typeInfo;
            return this;
        }

        public Builder withExtraMetaData(Map<String, String> extraMetaData) {
            this.extraMetaData = extraMetaData;
            return this;
        }

        protected Builder self() {
            return this;
        }

        protected WriteSupport<IValueReference> getWriteSupport(Configuration conf) {
            return new ObjectWriteSupport(this.type, this.typeInfo, this.extraMetaData);
        }
    }

}
