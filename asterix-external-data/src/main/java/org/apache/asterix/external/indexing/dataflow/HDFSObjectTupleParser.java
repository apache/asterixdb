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

import java.io.InputStream;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.indexing.input.AbstractHDFSReader;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.std.file.ITupleParser;

/*
 * This class is used with hdfs objects instead of hdfs
 */
public class HDFSObjectTupleParser implements ITupleParser{

    private ArrayTupleBuilder tb;
    private final FrameTupleAppender appender;
    private IAsterixHDFSRecordParser deserializer;

    public HDFSObjectTupleParser(IHyracksTaskContext ctx, ARecordType recType, IAsterixHDFSRecordParser deserializer) throws HyracksDataException {
        appender = new FrameTupleAppender(new VSizeFrame(ctx));
        this.deserializer = deserializer;
        tb = new ArrayTupleBuilder(1);
    }

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
        AbstractHDFSReader reader = (AbstractHDFSReader) in;
        Object object;
        try {
            reader.initialize();
            object = reader.readNext();
            while (object!= null) {
                tb.reset();
                deserializer.parse(object, tb.getDataOutput());
                tb.addFieldEndOffset();
                addTupleToFrame(writer);
                object = reader.readNext();
            }
            appender.flush(writer, true);
        } catch (AsterixException ae) {
            throw new HyracksDataException(ae);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    protected void addTupleToFrame(IFrameWriter writer) throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            appender.flush(writer, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

}
