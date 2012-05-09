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

import java.util.Map;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.operators.file.NtDelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataStreamParser extends AbstractStreamDataParser {

    protected Character delimiter = defaultDelimiter;

    protected static final Character defaultDelimiter = new Character('\n');

    public Character getDelimiter() {
        return delimiter;
    }

    public DelimitedDataStreamParser(Character delimiter) {
        this.delimiter = delimiter;
    }

    public DelimitedDataStreamParser() {
    }

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
        tupleParser = new NtDelimitedDataTupleParserFactory(recordType, fieldParserFactories, delimiter)
                .createTupleParser(ctx);
    }

    @Override
    public void parse(IFrameWriter writer) throws HyracksDataException {
        tupleParser.parse(inputStream, writer);
    }

    @Override
    public void configure(Map<String, String> configuration) {
        String delimiterArg = configuration.get(KEY_DELIMITER);
        if (delimiterArg != null) {
            delimiter = delimiterArg.charAt(0);
        } else {
            delimiter = '\n';
        }
    }

}
