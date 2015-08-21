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
package org.apache.asterix.external.dataset.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.feeds.api.IDatasourceAdapter;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public abstract class StreamBasedAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    protected static final Logger LOGGER = Logger.getLogger(StreamBasedAdapter.class.getName());

    public abstract InputStream getInputStream(int partition) throws IOException;

    protected final ITupleParser tupleParser;

    protected final IAType sourceDatatype;

    public StreamBasedAdapter(ITupleParserFactory parserFactory, IAType sourceDatatype, IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        this.tupleParser = parserFactory.createTupleParser(ctx);
        this.sourceDatatype = sourceDatatype;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        InputStream in = getInputStream(partition);
        if (in != null) {
            tupleParser.parse(in, writer);
        } else {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Could not obtain input stream for parsing from adapter " + this + "[" + partition + "]");
            }
        }
    }

}
