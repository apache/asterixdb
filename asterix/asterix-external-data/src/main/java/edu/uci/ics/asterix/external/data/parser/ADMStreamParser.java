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
import edu.uci.ics.asterix.runtime.operators.file.AdmSchemafullRecordParserFactory;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ADMStreamParser extends AbstractStreamDataParser {

    public ADMStreamParser() {
    }

    @Override
    public void initialize(ARecordType atype, IHyracksTaskContext ctx) {
        tupleParser = new AdmSchemafullRecordParserFactory(atype).createTupleParser(ctx);
    }

    @Override
    public void parse(IFrameWriter writer) throws HyracksDataException {
        tupleParser.parse(inputStream, writer);
    }

    @Override
    public void configure(Map<String, String> configuration) {
    }

}
