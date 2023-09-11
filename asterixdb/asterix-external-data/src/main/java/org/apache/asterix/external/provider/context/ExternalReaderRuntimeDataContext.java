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
package org.apache.asterix.external.provider.context;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.stream.StreamRecordReader;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class ExternalReaderRuntimeDataContext extends ExternalStreamRuntimeDataContext {
    private final IExternalFilterValueEmbedder valueEmbedder;
    private StreamRecordReader reader;

    public ExternalReaderRuntimeDataContext(IHyracksTaskContext context, int partition,
            IExternalFilterValueEmbedder valueEmbedder) {
        super(context, partition, valueEmbedder);
        this.valueEmbedder = valueEmbedder;
        reader = null;
    }

    @Override
    public Supplier<String> getDatasourceNameSupplier() {
        if (reader == null) {
            // Safeguard: cannot invoke this method unless a reader has been set
            throw new NullPointerException("Reader has not been set");
        }
        return reader.getDataSourceName();
    }

    @Override
    public LongSupplier getLineNumberSupplier() {
        if (reader == null) {
            // Safeguard: cannot invoke this method unless a reader has been set
            throw new NullPointerException("Reader has not been set");
        }
        return reader.getLineNumber();
    }

    @Override
    public IExternalFilterValueEmbedder getValueEmbedder() {
        return valueEmbedder;
    }

    public void setReader(StreamRecordReader reader) {
        this.reader = reader;
    }
}
