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
package org.apache.asterix.external.indexing.dataflow;

import java.util.Map;

import org.apache.asterix.external.adapter.factory.HDFSAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.ITupleParser;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

public class HDFSObjectTupleParserFactory implements ITupleParserFactory{
    private static final long serialVersionUID = 1L;
    // parser class name in case of binary format
    private String parserClassName;
    // the expected data type
    private ARecordType atype;
    // the hadoop job conf
    private HDFSAdapterFactory adapterFactory;
    // adapter arguments
    private Map<String,String> arguments;

    public HDFSObjectTupleParserFactory(ARecordType atype, HDFSAdapterFactory adapterFactory, Map<String,String> arguments){
        this.parserClassName = (String) arguments.get(HDFSAdapterFactory.KEY_PARSER);
        this.atype = atype;
        this.arguments = arguments;
        this.adapterFactory = adapterFactory;
    }

    @Override
    public ITupleParser createTupleParser(IHyracksTaskContext ctx) throws HyracksDataException {
        IAsterixHDFSRecordParser objectParser;
        if (parserClassName.equals(HDFSAdapterFactory.PARSER_HIVE)) {
            objectParser = new HiveObjectParser();
        } else {
            try {
                objectParser = (IAsterixHDFSRecordParser) Class.forName(parserClassName).newInstance();
            } catch (Exception e) {
                throw new HyracksDataException("Unable to create object parser", e);
            }
        }
        try {
            objectParser.initialize(atype, arguments, adapterFactory.getJobConf());
        } catch (Exception e) {
            throw new HyracksDataException("Unable to initialize object parser", e);
        }

        return new HDFSObjectTupleParser(ctx, atype, objectParser);
    }

}
