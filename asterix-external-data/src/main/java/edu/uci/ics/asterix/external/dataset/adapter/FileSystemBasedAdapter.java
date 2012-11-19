/*
 * Copyright 2009-2012 by The Regents of the University of California
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
import java.lang.reflect.Constructor;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.operators.file.NtDelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public abstract class FileSystemBasedAdapter extends AbstractDatasourceAdapter {

    protected boolean userDefinedParser = false;
    protected String parserFactoryClassname;

    public static final String KEY_DELIMITER = "delimiter";

    public abstract InputStream getInputStream(int partition) throws IOException;

    public FileSystemBasedAdapter(IAType atype) {
        this.atype = atype;
    }

    public FileSystemBasedAdapter() {
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        InputStream in = getInputStream(partition);
        ITupleParser parser = getTupleParser();
        parser.parse(in, writer);
    }

    @Override
    public abstract void initialize(IHyracksTaskContext ctx) throws Exception;

    @Override
    public abstract void configure(Map<String, String> arguments) throws Exception;

    @Override
    public abstract AdapterType getAdapterType();

    protected ITupleParser getTupleParser() throws Exception {
        ITupleParser parser = null;
        if (userDefinedParser) {
            Class tupleParserFactoryClass = Class.forName(parserFactoryClassname);
            ITupleParserFactory parserFactory = (ITupleParserFactory) tupleParserFactoryClass.newInstance();
            parser = parserFactory.createTupleParser(ctx);
        } else {
            if (FORMAT_DELIMITED_TEXT.equalsIgnoreCase(configuration.get(KEY_FORMAT))) {
                parser = getDelimitedDataTupleParser((ARecordType) atype);

            } else if (FORMAT_ADM.equalsIgnoreCase(configuration.get(KEY_FORMAT))) {
                parser = getADMDataTupleParser((ARecordType) atype);
            } else {
                throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT) + " not supported");
            }
        }
        return parser;

    }

    protected void configureFormat() throws Exception {
        parserFactoryClassname = configuration.get(KEY_PARSER_FACTORY);
        userDefinedParser = (parserFactoryClassname != null);

        if (parserFactoryClassname == null) {
            if (FORMAT_DELIMITED_TEXT.equalsIgnoreCase(configuration.get(KEY_FORMAT))) {
                parserFactoryClassname = formatToParserFactoryMap.get(FORMAT_DELIMITED_TEXT);
            } else if (FORMAT_ADM.equalsIgnoreCase(configuration.get(KEY_FORMAT))) {
                parserFactoryClassname = formatToParserFactoryMap.get(FORMAT_ADM);
            } else {
                throw new IllegalArgumentException(" format " + configuration.get(KEY_FORMAT) + " not supported");
            }
        }

    }

    protected ITupleParser getDelimitedDataTupleParser(ARecordType recordType) throws AsterixException {
        ITupleParser parser;
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
        String delimiterValue = (String) configuration.get(KEY_DELIMITER);
        if (delimiterValue != null && delimiterValue.length() > 1) {
            throw new AsterixException("improper delimiter");
        }

        Character delimiter = delimiterValue.charAt(0);
        parser = new NtDelimitedDataTupleParserFactory(recordType, fieldParserFactories, delimiter)
                .createTupleParser(ctx);
        return parser;
    }

    protected ITupleParser getADMDataTupleParser(ARecordType recordType) throws AsterixException {
        try {
            Class tupleParserFactoryClass = Class.forName(parserFactoryClassname);
            Constructor ctor = tupleParserFactoryClass.getConstructor(ARecordType.class);
            ITupleParserFactory parserFactory = (ITupleParserFactory) ctor.newInstance(atype);
            return parserFactory.createTupleParser(ctx);
        } catch (Exception e) {
            throw new AsterixException(e);
        }

    }
}
