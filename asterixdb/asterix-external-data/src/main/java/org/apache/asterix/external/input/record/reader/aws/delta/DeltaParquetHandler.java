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
package org.apache.asterix.external.input.record.reader.aws.delta;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultParquetHandler;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

public class DeltaParquetHandler extends DefaultParquetHandler {

    private final FileIO fileIO;
    private final Configuration conf;

    public DeltaParquetHandler(FileIO fileIO, Configuration conf) {
        super(fileIO);
        this.fileIO = fileIO;
        this.conf = conf;
    }

    public CloseableIterator<ColumnarBatch> readParquetSplits(final CloseableIterator<SerializableFileSplit> splits,
            final StructType physicalSchema, final Optional<Predicate> predicate) throws IOException {
        return new CloseableIterator<>() {
            private final DeltaParquetFileReader batchReader;
            private CloseableIterator<ColumnarBatch> currentFileReader;

            {
                this.batchReader =
                        new DeltaParquetFileReader(DeltaParquetHandler.this.fileIO, DeltaParquetHandler.this.conf);
            }

            public void close() throws IOException {
                Utils.closeCloseables(new AutoCloseable[] { this.currentFileReader, splits });
            }

            public boolean hasNext() {
                if (this.currentFileReader != null && this.currentFileReader.hasNext()) {
                    return true;
                } else {
                    Utils.closeCloseables(new AutoCloseable[] { this.currentFileReader });
                    this.currentFileReader = null;
                    if (splits.hasNext()) {
                        this.currentFileReader = this.batchReader.read(splits.next(), physicalSchema, predicate);
                        return this.hasNext();
                    } else {
                        return false;
                    }
                }
            }

            public ColumnarBatch next() {
                return this.currentFileReader.next();
            }
        };
    }
}
