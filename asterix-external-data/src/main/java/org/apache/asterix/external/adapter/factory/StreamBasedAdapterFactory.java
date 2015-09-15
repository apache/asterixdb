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
package org.apache.asterix.external.adapter.factory;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.asterix.external.util.INodeResolver;
import org.apache.asterix.metadata.external.IAdapterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory.InputDataFormat;
import org.apache.hyracks.dataflow.std.file.ITupleParserFactory;

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
