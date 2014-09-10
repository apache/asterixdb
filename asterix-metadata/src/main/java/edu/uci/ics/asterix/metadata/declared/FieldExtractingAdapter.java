package edu.uci.ics.asterix.metadata.declared;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.metadata.feeds.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class FieldExtractingAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = 1L;

    private final RecordDescriptor inRecDesc;

    private final RecordDescriptor outRecDesc;

    private final IDatasourceAdapter wrappedAdapter;

    private final FieldExtractingPushRuntime fefw;

    public FieldExtractingAdapter(IHyracksTaskContext ctx, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            int[] extractFields, ARecordType rType, IDatasourceAdapter wrappedAdapter) {
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;
        this.wrappedAdapter = wrappedAdapter;
        fefw = new FieldExtractingPushRuntime(ctx, extractFields, rType);
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        fefw.setInputRecordDescriptor(0, inRecDesc);
        fefw.setFrameWriter(0, writer, outRecDesc);
        fefw.open();
        try {
            wrappedAdapter.start(partition, fefw);
        } catch (Throwable t) {
            fefw.fail();
            throw t;
        } finally {
            fefw.close();
        }
    }

    private static class FieldExtractingPushRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {

        private final IHyracksTaskContext ctx;

        private final int[] extractFields;

        private final ARecordType rType;

        private final int nullBitmapSize;

        private final ArrayTupleBuilder tb;

        public FieldExtractingPushRuntime(IHyracksTaskContext ctx, int[] extractFields, ARecordType rType) {
            this.ctx = ctx;
            this.extractFields = extractFields;
            this.rType = rType;
            nullBitmapSize = ARecordType.computeNullBitmapSize(rType);
            tb = new ArrayTupleBuilder(extractFields.length + 1);
        }

        @Override
        public void open() throws HyracksDataException {
            initAccessAppendRef(ctx);
        }

        @Override
        public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
            tAccess.reset(buffer);
            for (int i = 0; i < tAccess.getTupleCount(); ++i) {
                tb.reset();
                tRef.reset(tAccess, i);
                byte[] record = tRef.getFieldData(0);
                int recStart = tRef.getFieldStart(0);
                for (int f = 0; f < extractFields.length; ++f) {
                    try {
                        int fOffset = ARecordSerializerDeserializer.getFieldOffsetById(record, recStart,
                                extractFields[f], nullBitmapSize, rType.isOpen());
                        if (fOffset == 0) {
                            tb.getDataOutput().write(ATypeTag.NULL.serialize());
                        } else {
                            IAType fType = rType.getFieldTypes()[extractFields[f]];
                            int fLen;
                            try {
                                fLen = NonTaggedFormatUtil.getFieldValueLength(record, recStart + fOffset,
                                        fType.getTypeTag(), false);
                            } catch (AsterixException e) {
                                throw new HyracksDataException(e);
                            }
                            tb.getDataOutput().write(fType.getTypeTag().serialize());
                            tb.getDataOutput().write(record, recStart + fOffset, fLen);
                        }
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    tb.addFieldEndOffset();
                }
                tb.addField(record, recStart, tRef.getFieldLength(0));
                appendToFrameFromTupleBuilder(tb);
            }
        }

        @Override
        public void close() throws HyracksDataException {
            flushIfNotFailed();
        }
    }

}
