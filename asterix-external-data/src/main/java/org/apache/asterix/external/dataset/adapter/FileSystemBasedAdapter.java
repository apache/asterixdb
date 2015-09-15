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
package org.apache.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;

import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public abstract class FileSystemBasedAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    public static final String NODE_RESOLVER_FACTORY_PROPERTY = "node.Resolver";

    public abstract InputStream getInputStream(int partition) throws IOException;

    protected final ITupleParserFactory parserFactory;
    protected ITupleParser tupleParser;
    protected final IAType sourceDatatype;
    protected IHyracksTaskContext ctx;

    public FileSystemBasedAdapter(ITupleParserFactory parserFactory, IAType sourceDatatype, IHyracksTaskContext ctx)
            throws HyracksDataException {
        this.parserFactory = parserFactory;
        this.sourceDatatype = sourceDatatype;
        this.ctx = ctx;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        tupleParser = parserFactory.createTupleParser(ctx);
        InputStream in = getInputStream(partition);
        tupleParser.parse(in, writer);
    }

    public String getFilename(int partition) {
        return null;
    }
}
