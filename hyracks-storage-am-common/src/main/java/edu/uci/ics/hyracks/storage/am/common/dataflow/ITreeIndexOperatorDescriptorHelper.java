package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public interface ITreeIndexOperatorDescriptorHelper extends IActivity {
	public IFileSplitProvider getTreeIndexFileSplitProvider();

	public IBinaryComparatorFactory[] getTreeIndexComparatorFactories();

	public IPrimitiveValueProviderFactory[] getTreeIndexValueProviderFactories();

	public ITypeTrait[] getTreeIndexTypeTraits();

	public ITreeIndexFrameFactory getTreeIndexInteriorFactory();

	public ITreeIndexFrameFactory getTreeIndexLeafFactory();

	public IStorageManagerInterface getStorageManager();

	public IIndexRegistryProvider<ITreeIndex> getTreeIndexRegistryProvider();

	public RecordDescriptor getRecordDescriptor();

	public ITreeIndexOpHelperFactory getTreeIndexOpHelperFactory();
}