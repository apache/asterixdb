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
package org.apache.asterix.external.input.record.reader.factory;

import java.util.Map;

import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.input.record.reader.AbstractStreamRecordReaderFactory;
import org.apache.asterix.external.input.record.reader.LineRecordReader;
import org.apache.asterix.external.input.record.reader.QuotedLineRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class LineRecordReaderFactory extends AbstractStreamRecordReaderFactory<char[]> {

    private static final long serialVersionUID = 1L;

    @Override
    public IRecordReader<? extends char[]> createRecordReader(IHyracksTaskContext ctx, int partition) throws Exception {
        String quoteString = configuration.get(ExternalDataConstants.KEY_QUOTE);
        LineRecordReader recordReader;
        if (quoteString != null) {
            recordReader = new QuotedLineRecordReader();
        } else {
            recordReader = new LineRecordReader();
        }
        return configureReader(recordReader, ctx, partition);
    }

    @Override
    public Class<? extends char[]> getRecordClass() {
        return char[].class;
    }

    @Override
    protected void configureStreamReaderFactory(Map<String, String> configuration) throws Exception {
    }

}
