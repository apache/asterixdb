package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;

public class BTreeFileEnlistmentOperatorNodePushable extends AbstractOperatorNodePushable {
	
	private final BTreeOpHelper btreeOpHelper;
	
	public BTreeFileEnlistmentOperatorNodePushable(AbstractBTreeOperatorDescriptor opDesc, IHyracksContext ctx, int partition) {
		btreeOpHelper = new BTreeOpHelper(opDesc, ctx, partition, BTreeOpHelper.BTreeMode.ENLIST_BTREE);
	}

	@Override
	public void deinitialize() throws HyracksDataException {
	}

	@Override
	public int getInputArity() {		
		return 0;
	}

	@Override
	public IFrameWriter getInputFrameWriter(int index) {
		return null;
	}

	@Override
	public void initialize() throws HyracksDataException {
		btreeOpHelper.init();
		btreeOpHelper.deinit();
	}

	@Override
	public void setOutputFrameWriter(int index, IFrameWriter writer,
			RecordDescriptor recordDesc) {	
	}	
}
