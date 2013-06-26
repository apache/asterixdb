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
package edu.uci.ics.pregelix.runtime.function;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;

/**
 * No operation update function factory
 * 
 * @author yingyib
 */
public class NoOpUpdateFunctionFactory implements IUpdateFunctionFactory {
    private static final long serialVersionUID = 1L;
    public static NoOpUpdateFunctionFactory INSTANCE = new NoOpUpdateFunctionFactory();

    private NoOpUpdateFunctionFactory() {

    }

    @Override
    public IUpdateFunction createFunction() {
        return new IUpdateFunction() {

            @Override
            public void open(IHyracksTaskContext ctx, RecordDescriptor rd, IFrameWriter... writer)
                    throws HyracksDataException {

            }

            @Override
            public void process(Object[] tuple) throws HyracksDataException {

            }

            @Override
            public void close() throws HyracksDataException {

            }

            @Override
            public void update(ITupleReference tupleRef, ArrayTupleBuilder cloneUpdateTb) throws HyracksDataException {

            }

        };
    }

}
