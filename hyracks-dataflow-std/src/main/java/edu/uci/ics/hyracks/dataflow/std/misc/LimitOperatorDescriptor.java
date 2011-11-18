package edu.uci.ics.hyracks.dataflow.std.misc;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class LimitOperatorDescriptor extends
		AbstractSingleActivityOperatorDescriptor {
	private static final long serialVersionUID = 1L;
	private final int outputLimit;

	public LimitOperatorDescriptor(JobSpecification spec,
			RecordDescriptor rDesc, int outputLimit) {
		super(spec, 1, 1);
		recordDescriptors[0] = rDesc;
		this.outputLimit = outputLimit;
	}

	@Override
	public IOperatorNodePushable createPushRuntime(
			final IHyracksTaskContext ctx, IOperatorEnvironment env,
			final IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) throws HyracksDataException {

		return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
			private FrameTupleAccessor fta;
			private int currentSize;
			private boolean finished;

			@Override
			public void open() throws HyracksDataException {
				fta = new FrameTupleAccessor(ctx.getFrameSize(),
						recordDescriptors[0]);
				currentSize = 0;
				finished = false;
				writer.open();
			}

			@Override
			public void nextFrame(ByteBuffer buffer)
					throws HyracksDataException {
				if (!finished) {
					fta.reset(buffer);
					int count = fta.getTupleCount();
					if ((currentSize + count) > outputLimit) {
						ByteBuffer b = ctx.allocateFrame();
						FrameTupleAppender partialAppender = new FrameTupleAppender(
								ctx.getFrameSize());
						partialAppender.reset(b, true);
						int copyCount = outputLimit - currentSize;
						for (int i = 0; i < copyCount; i++) {
							partialAppender.append(fta, i);
							currentSize++;
						}
						FrameUtils.makeReadable(b);
						FrameUtils.flushFrame(b, writer);
						finished = true;
					} else {
						FrameUtils.flushFrame(buffer, writer);
						currentSize += count;
					}
				}
			}

			@Override
			public void fail() throws HyracksDataException {
				writer.fail();

			}

			@Override
			public void close() throws HyracksDataException {
				writer.close();
			}
		};
	}

}
