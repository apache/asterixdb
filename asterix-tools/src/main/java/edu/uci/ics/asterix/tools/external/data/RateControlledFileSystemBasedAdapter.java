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
package edu.uci.ics.asterix.tools.external.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import edu.uci.ics.asterix.common.feeds.api.IFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * An adapter that simulates a feed from the contents of a source file. The file
 * can be on the local file system or on HDFS. The feed ends when the content of
 * the source file has been ingested.
 */

public class RateControlledFileSystemBasedAdapter extends FileSystemBasedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;
    private FileSystemBasedAdapter coreAdapter;

    public RateControlledFileSystemBasedAdapter(ARecordType atype, Map<String, String> configuration,
            FileSystemBasedAdapter coreAdapter, String format, ITupleParserFactory parserFactory,
            IHyracksTaskContext ctx) throws Exception {
        super(parserFactory, atype, ctx);
        this.coreAdapter = coreAdapter;
    }

    @Override
    public InputStream getInputStream(int partition) throws IOException {
        return coreAdapter.getInputStream(partition);
    }

    @Override
    public void stop() {
       // ((RateControlledTupleParser) tupleParser).stop();
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PULL;
    }

    @Override
    public boolean handleException(Exception e) {
        return false;
    }

}
