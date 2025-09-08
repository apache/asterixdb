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

import static io.delta.kernel.defaults.internal.parquet.ParquetFilterUtils.toParquetFilter;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.hadoop.ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.DICTIONARY_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.RECORD_FILTERING_ENABLED;
import static org.apache.parquet.hadoop.ParquetInputFormat.setFilterPredicate;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.internal.parquet.ParquetFileReader;
import io.delta.kernel.exceptions.KernelEngineException;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

public class DeltaParquetFileReader extends ParquetFileReader {

    private final FileIO fileIO;
    private final int maxBatchSize;
    private final Configuration conf;

    public DeltaParquetFileReader(FileIO fileIO, Configuration conf) {
        super(fileIO);
        this.fileIO = requireNonNull(fileIO, "fileIO is null");
        this.conf = requireNonNull(conf, "conf is null");
        this.maxBatchSize =
                fileIO.getConf("delta.kernel.default.parquet.reader.batch-size").map(Integer::valueOf).orElse(1024);
    }

    public CloseableIterator<ColumnarBatch> read(InputSplit split, StructType schema, Optional<Predicate> predicate) {

        final boolean hasRowIndexCol = schema.indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME) >= 0
                && schema.get(StructField.METADATA_ROW_INDEX_COLUMN_NAME).isMetadataColumn();

        return new CloseableIterator<ColumnarBatch>() {
            private final BatchReadSupport readSupport = new BatchReadSupport(maxBatchSize, schema);
            private ParquetRecordReader<Object> reader;
            private boolean hasNotConsumedNextElement;

            @Override
            public void close() throws IOException {
                Utils.closeCloseables(reader);
            }

            @Override
            public boolean hasNext() {
                initParquetReaderIfRequired();
                try {
                    if (hasNotConsumedNextElement) {
                        return true;
                    }

                    hasNotConsumedNextElement = reader.nextKeyValue() && reader.getCurrentValue() != null;
                    return hasNotConsumedNextElement;
                } catch (IOException | InterruptedException ex) {
                    throw new KernelEngineException("Error reading Parquet file: " + ((FileSplit) split).getPath(), ex);
                }
            }

            @Override
            public ColumnarBatch next() {
                if (!hasNotConsumedNextElement) {
                    throw new NoSuchElementException();
                }
                int batchSize = 0;
                do {
                    hasNotConsumedNextElement = false;
                    // hasNext reads to row to confirm there is a next element.
                    // get the row index only if required by the read schema
                    long rowIndex = 0;
                    try {
                        rowIndex = hasRowIndexCol ? reader.getCurrentRowIndex() : -1;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    readSupport.finalizeCurrentRow(rowIndex);
                    batchSize++;
                } while (batchSize < maxBatchSize && hasNext());

                return readSupport.getDataAsColumnarBatch(batchSize);
            }

            private void initParquetReaderIfRequired() {
                if (reader == null) {
                    org.apache.parquet.hadoop.ParquetFileReader fileReader = null;
                    try {
                        Configuration confCopy = conf;
                        Path filePath = ((FileSplit) split).getPath();

                        // We need physical schema in order to construct a filter that can be
                        // pushed into the `parquet-mr` reader. For that reason read the footer
                        // in advance.
                        ParquetMetadata footer =
                                org.apache.parquet.hadoop.ParquetFileReader.readFooter(confCopy, filePath);

                        MessageType parquetSchema = footer.getFileMetaData().getSchema();
                        Optional<FilterPredicate> parquetPredicate =
                                predicate.flatMap(predicate -> toParquetFilter(parquetSchema, predicate));

                        if (parquetPredicate.isPresent()) {
                            // clone the configuration to avoid modifying the original one
                            confCopy = new Configuration(confCopy);

                            setFilterPredicate(confCopy, parquetPredicate.get());
                            // Disable the record level filtering as the `parquet-mr` evaluates
                            // the filter once the entire record has been materialized. Instead,
                            // we use the predicate to prune the row groups which is more efficient.
                            // In the future, we can consider using the record level filtering if a
                            // native Parquet reader is implemented in Kernel default module.
                            confCopy.set(RECORD_FILTERING_ENABLED, "false");
                            confCopy.set(DICTIONARY_FILTERING_ENABLED, "false");
                            confCopy.set(COLUMN_INDEX_FILTERING_ENABLED, "false");
                        }

                        // Pass the already read footer to the reader to avoid reading it again.
                        fileReader = new ParquetFileReaderWithFooter(filePath, confCopy, footer);
                        reader = new ParquetRecordReader<>(readSupport, ParquetInputFormat.getFilter(confCopy));
                        reader.initialize((FileSplit) split, confCopy, Reporter.NULL);
                    } catch (IOException e) {
                        Utils.closeCloseablesSilently(fileReader, reader);
                        throw new KernelEngineException("Error reading Parquet file: " + ((FileSplit) split).getPath(),
                                e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    /**
     * Wrapper around {@link org.apache.parquet.hadoop.ParquetFileReader} to allow using the
     * provided footer instead of reading it again. We read the footer in advance to construct a
     * predicate for filtering rows.
     */
    private static class ParquetFileReaderWithFooter extends org.apache.parquet.hadoop.ParquetFileReader {
        private final ParquetMetadata footer;

        ParquetFileReaderWithFooter(Path filePath, Configuration configuration, ParquetMetadata footer)
                throws IOException {
            super(configuration, filePath, footer);
            this.footer = requireNonNull(footer, "footer is null");
        }

        @Override
        public ParquetMetadata getFooter() {
            return footer; // return the footer passed in the constructor
        }
    }
}
