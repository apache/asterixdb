/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;

import edu.uci.ics.asterix.common.feeds.api.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

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
