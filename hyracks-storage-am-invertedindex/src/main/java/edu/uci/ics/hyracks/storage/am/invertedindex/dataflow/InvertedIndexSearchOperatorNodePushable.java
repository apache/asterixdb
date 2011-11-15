package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import java.io.DataOutput;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.OccurrenceThresholdPanicException;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.SearchResultCursor;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.TOccurrenceSearcher;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;

public class InvertedIndexSearchOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final TreeIndexOpHelper btreeOpHelper;
    private final InvertedIndexOpHelper invIndexOpHelper;
    private final IHyracksTaskContext ctx;
    private final int queryField;
    private FrameTupleAccessor accessor;
    private FrameTupleReference tuple;
    private IRecordDescriptorProvider recordDescProvider;

    private final IInvertedIndexSearchModifier searchModifier;
    private final IBinaryTokenizer queryTokenizer;
    private TOccurrenceSearcher searcher;
    private IInvertedIndexResultCursor resultCursor;

    private ByteBuffer writeBuffer;
    private FrameTupleAppender appender;
    private ArrayTupleBuilder tb;
    private DataOutput dos;

    private final AbstractInvertedIndexOperatorDescriptor opDesc;

    public InvertedIndexSearchOperatorNodePushable(AbstractInvertedIndexOperatorDescriptor opDesc,
            IHyracksTaskContext ctx, int partition, int queryField, IInvertedIndexSearchModifier searchModifier,
            IBinaryTokenizer queryTokenizer, IRecordDescriptorProvider recordDescProvider) {
        this.opDesc = opDesc;
        btreeOpHelper = opDesc.getTreeIndexOpHelperFactory().createTreeIndexOpHelper(opDesc, ctx, partition,
                IndexHelperOpenMode.CREATE);
        invIndexOpHelper = new InvertedIndexOpHelper(btreeOpHelper, opDesc, ctx, partition);
        this.ctx = ctx;
        this.queryField = queryField;
        this.searchModifier = searchModifier;
        this.queryTokenizer = queryTokenizer;
        this.recordDescProvider = recordDescProvider;
    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        accessor = new FrameTupleAccessor(btreeOpHelper.getHyracksTaskContext().getFrameSize(), recDesc);
        tuple = new FrameTupleReference();
        // BTree.
        try {
            btreeOpHelper.init();
            btreeOpHelper.getTreeIndex().open(btreeOpHelper.getIndexFileId());
        } catch (Exception e) {
            // Cleanup in case of failure/
            btreeOpHelper.deinit();
            if (e instanceof HyracksDataException) {
                throw (HyracksDataException) e;
            } else {
                throw new HyracksDataException(e);
            }
        }
        // Inverted Index.
        try {
            invIndexOpHelper.init();
            invIndexOpHelper.getInvIndex().open(invIndexOpHelper.getInvIndexFileId());
        } catch (Exception e) {
            // Cleanup in case of failure.
            invIndexOpHelper.deinit();
            if (e instanceof HyracksDataException) {
                throw (HyracksDataException) e;
            } else {
                throw new HyracksDataException(e);
            }
        }

        writeBuffer = btreeOpHelper.getHyracksTaskContext().allocateFrame();
        tb = new ArrayTupleBuilder(opDesc.getInvListsTypeTraits().length);
        dos = tb.getDataOutput();
        appender = new FrameTupleAppender(btreeOpHelper.getHyracksTaskContext().getFrameSize());
        appender.reset(writeBuffer, true);

        searcher = new TOccurrenceSearcher(ctx, invIndexOpHelper.getInvIndex(), queryTokenizer);
        resultCursor = new SearchResultCursor(searcher.createResultFrameTupleAccessor(),
                searcher.createResultTupleReference());

        writer.open();
    }

    private void writeSearchResults() throws Exception {
        while (resultCursor.hasNext()) {
            resultCursor.next();
            tb.reset();
            ITupleReference invListElement = resultCursor.getTuple();
            int invListFields = opDesc.getInvListsTypeTraits().length;
            for (int i = 0; i < invListFields; i++) {
                dos.write(invListElement.getFieldData(i), invListElement.getFieldStart(i),
                        invListElement.getFieldLength(i));
                tb.addFieldEndOffset();
            }
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                FrameUtils.flushFrame(writeBuffer, writer);
                appender.reset(writeBuffer, true);
                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                    throw new IllegalStateException();
                }
            }
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                tuple.reset(accessor, i);
                searcher.reset();
                try {
                    searcher.reset();
                    searcher.search(resultCursor, tuple, queryField, searchModifier);
                    writeSearchResults();
                } catch (OccurrenceThresholdPanicException e) {
                    // Ignore panic cases for now.
                }
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writeBuffer, writer);
            }
            writer.close();
        } finally {
            try {
                btreeOpHelper.deinit();
            } finally {
                invIndexOpHelper.deinit();
            }
        }
    }
}
