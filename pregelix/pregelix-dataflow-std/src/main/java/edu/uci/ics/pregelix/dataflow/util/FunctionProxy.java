package edu.uci.ics.pregelix.dataflow.util;

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
        inputRd = inputRdFactory.createRecordDescriptor();
        tupleDe = new TupleDeserializer(inputRd);
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
     * @param tb
     *            input data
     * @param updateRef
     *            update pointer
     * @throws HyracksDataException
     */
    public void functionCall(ArrayTupleBuilder tb, ITupleReference updateRef) throws HyracksDataException {
        Object[] tuple = tupleDe.deserializeRecord(tb);
        function.process(tuple);
        function.update(updateRef);
    }

    /**
     * call function, without the newly generated tuple, just the tuple in btree
     * 
     * @param updateRef
     * @throws HyracksDataException
     */
    public void functionCall(ITupleReference updateRef) throws HyracksDataException {
        Object[] tuple = tupleDe.deserializeRecord(updateRef);
        function.process(tuple);
        function.update(updateRef);
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
    }
}
