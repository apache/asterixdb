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
package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.asterix.external.util.INodeResolver;
import edu.uci.ics.asterix.metadata.external.IAdapterFactory;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public abstract class StreamBasedAdapterFactory implements IAdapterFactory {

    private static final long serialVersionUID = 1L;
    protected static final Logger LOGGER = Logger.getLogger(StreamBasedAdapterFactory.class.getName());

    protected static INodeResolver nodeResolver;

    protected Map<String, String> configuration;
    protected ITupleParserFactory parserFactory;

    public abstract InputDataFormat getInputDataFormat();

    protected void configureFormat(IAType sourceDatatype) throws Exception {
        parserFactory = new AsterixTupleParserFactory(configuration, (ARecordType) sourceDatatype, getInputDataFormat());

    }

}
