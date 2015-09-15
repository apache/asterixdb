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
package org.apache.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.parse.ITupleForwardPolicy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.std.file.ITupleParser;

/**
 * An abstract class implementation for ITupleParser. It provides common
 * functionality involved in parsing data in an external format and packing
 * frames with formed tuples.
 */
public abstract class AbstractTupleParser implements ITupleParser {

    protected static Logger LOGGER = Logger.getLogger(AbstractTupleParser.class.getName());

    protected ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
    protected DataOutput dos = tb.getDataOutput();
    protected final ARecordType recType;
    protected final IHyracksTaskContext ctx;

    public AbstractTupleParser(IHyracksTaskContext ctx, ARecordType recType) throws HyracksDataException {
        this.recType = recType;
        this.ctx = ctx;
    }

    public abstract IDataParser getDataParser();

    public abstract ITupleForwardPolicy getTupleParserPolicy();

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
        IDataParser parser = getDataParser();
        ITupleForwardPolicy policy = getTupleParserPolicy();
        try {
            parser.initialize(in, recType, true);
            policy.initialize(ctx, writer);
            while (true) {
                tb.reset();
                if (!parser.parse(tb.getDataOutput())) {
                    break;
                }
                tb.addFieldEndOffset();
                policy.addTuple(tb);
            }
            policy.close();
        } catch (AsterixException ae) {
            throw new HyracksDataException(ae);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

}
