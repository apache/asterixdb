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
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class ManagedDelimitedDataStreamParser extends DelimitedDataStreamParser implements IManagedDataParser {

    private IManagedFeedAdapter adapter;
   
    @Override
    public void initialize(ARecordType recordType, IHyracksTaskContext ctx) {
        int n = recordType.getFieldTypes().length;
        IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
        for (int i = 0; i < n; i++) {
            ATypeTag tag = recordType.getFieldTypes()[i].getTypeTag();
            IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
            if (vpf == null) {
                throw new NotImplementedException("No value parser factory for delimited fields of type " + tag);
            }
            fieldParserFactories[i] = vpf;
        }
        tupleParser = new ManagedDelimitedDataRecordParserFactory(fieldParserFactories, delimiter.charValue(),
                recordType, adapter).createTupleParser(ctx);
    }

    @Override
    public IManagedTupleParser getManagedTupleParser() {
        return (IManagedTupleParser) tupleParser;
    }

    @Override
    public void setAdapter(IManagedFeedAdapter adapter) {
        this.adapter = adapter;
    }
}
