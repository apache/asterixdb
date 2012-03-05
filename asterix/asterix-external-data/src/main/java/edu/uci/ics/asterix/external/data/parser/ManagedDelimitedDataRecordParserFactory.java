/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.data.parser;

import edu.uci.ics.asterix.feed.managed.adapter.IManagedFeedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.NtDelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;

public class ManagedDelimitedDataRecordParserFactory extends NtDelimitedDataTupleParserFactory {

    private final IManagedFeedAdapter adapter;

    public ManagedDelimitedDataRecordParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter,
            ARecordType recType, IManagedFeedAdapter adapter) {
        super(recType, fieldParserFactories, fieldDelimiter);
        this.adapter = adapter;
    }
    
    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
        return new ManagedDelimitedDataTupleParser(ctx, recordType, adapter, valueParserFactories, fieldDelimiter);
    }
}
