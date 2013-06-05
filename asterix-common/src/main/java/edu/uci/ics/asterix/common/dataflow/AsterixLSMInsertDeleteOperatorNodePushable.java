package edu.uci.ics.asterix.common.dataflow;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMIndexInsertUpdateDeleteOperatorNodePushable;

public class AsterixLSMInsertDeleteOperatorNodePushable extends LSMIndexInsertUpdateDeleteOperatorNodePushable {

    private final boolean isPrimary;

    public AsterixLSMInsertDeleteOperatorNodePushable(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, int[] fieldPermutation, IRecordDescriptorProvider recordDescProvider, IndexOperation op,
            boolean isPrimary) {
        super(opDesc, ctx, partition, fieldPermutation, recordDescProvider, op);
        this.isPrimary = isPrimary;
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        boolean first = true;
        accessor.reset(buffer);
        ILSMIndexAccessor lsmAccessor = (ILSMIndexAccessor) indexAccessor;
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                if (tupleFilter != null) {
                    frameTuple.reset(accessor, i);
                    if (!tupleFilter.accept(frameTuple)) {
                        lsmAccessor.noOp();
                        continue;
                    }
                }
                tuple.reset(accessor, i);
                switch (op) {
                    case INSERT:
                        if (first && isPrimary) {
                            lsmAccessor.insert(tuple);
                            first = false;
                        } else {
                            lsmAccessor.forceInsert(tuple);
                        }
                        break;
                    case DELETE:
                        if (first && isPrimary) {
                            lsmAccessor.delete(tuple);
                            first = false;
                        } else {
                            lsmAccessor.forceDelete(tuple);
                        }
                        break;
                    default: {
                        throw new HyracksDataException("Unsupported operation " + op
                                + " in tree index InsertDelete operator");
                    }
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        System.arraycopy(buffer.array(), 0, writeBuffer.array(), 0, buffer.capacity());
        FrameUtils.flushFrame(writeBuffer, writer);
    }

}
