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

package edu.uci.ics.pregelix.dataflow.util;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRuntimeHookFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunction;
import edu.uci.ics.pregelix.dataflow.std.base.IUpdateFunctionFactory;

public class FunctionProxy {

    private final IUpdateFunction function;
    private final IRuntimeHookFactory preHookFactory;
    private final IRuntimeHookFactory postHookFactory;
    private final IRecordDescriptorFactory inputRdFactory;
    private final IHyracksTaskContext ctx;
    private final IFrameWriter[] writers;
    private TupleDeserializer tupleDe;
    private RecordDescriptor inputRd;
    private ClassLoader ctxCL;

    public FunctionProxy(IHyracksTaskContext ctx, IUpdateFunctionFactory functionFactory,
            IRuntimeHookFactory preHookFactory, IRuntimeHookFactory postHookFactory,
            IRecordDescriptorFactory inputRdFactory, IFrameWriter[] writers) {
        this.function = functionFactory.createFunction();
        this.preHookFactory = preHookFactory;
        this.postHookFactory = postHookFactory;
        this.inputRdFactory = inputRdFactory;
        this.writers = writers;
        this.ctx = ctx;
    }

    /**
     * Initialize the function
     * 
     * @throws HyracksDataException
     */
    public void functionOpen() throws HyracksDataException {
        inputRd = inputRdFactory.createRecordDescriptor(ctx);
        tupleDe = new TupleDeserializer(inputRd);
        ctxCL = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        for (IFrameWriter writer : writers) {
            writer.open();
        }
        if (preHookFactory != null)
            preHookFactory.createRuntimeHook().configure(ctx);
        function.open(ctx, inputRd, writers);
    }

    /**
     * Call the function
     * 
     * @param leftAccessor
     *            input page accessor
     * @param leftTupleIndex
     *            the tuple index in the page
     * @param updateRef
     *            update pointer
     * @throws HyracksDataException
     */
    public void functionCall(IFrameTupleAccessor leftAccessor, int leftTupleIndex, ITupleReference right,
            ArrayTupleBuilder cloneUpdateTb) throws HyracksDataException {
        Object[] tuple = tupleDe.deserializeRecord(leftAccessor, leftTupleIndex, right);
        function.process(tuple);
        function.update(right, cloneUpdateTb);
    }

    /**
     * call function, without the newly generated tuple, just the tuple in btree
     * 
     * @param updateRef
     * @throws HyracksDataException
     */
    public void functionCall(ITupleReference updateRef, ArrayTupleBuilder cloneUpdateTb) throws HyracksDataException {
        Object[] tuple = tupleDe.deserializeRecord(updateRef);
        function.process(tuple);
        function.update(updateRef, cloneUpdateTb);
    }

    /**
     * Call the function
     * 
     * @param tb
     *            input data
     * @param inPlaceUpdateRef
     *            update pointer
     * @throws HyracksDataException
     */
    public void functionCall(ArrayTupleBuilder tb, ITupleReference inPlaceUpdateRef, ArrayTupleBuilder cloneUpdateTb)
            throws HyracksDataException {
        Object[] tuple = tupleDe.deserializeRecord(tb, inPlaceUpdateRef);
        function.process(tuple);
        function.update(inPlaceUpdateRef, cloneUpdateTb);
    }

    /**
     * Close the function
     * 
     * @throws HyracksDataException
     */
    public void functionClose() throws HyracksDataException {
        if (postHookFactory != null)
            postHookFactory.createRuntimeHook().configure(ctx);
        function.close();
        for (IFrameWriter writer : writers) {
            writer.close();
        }
        Thread.currentThread().setContextClassLoader(ctxCL);
    }
}
