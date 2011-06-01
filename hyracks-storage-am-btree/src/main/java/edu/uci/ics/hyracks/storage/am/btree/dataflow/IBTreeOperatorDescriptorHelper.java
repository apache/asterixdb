package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public interface IBTreeOperatorDescriptorHelper extends IActivityNode {
    public IFileSplitProvider getBTreeFileSplitProvider();

    public IBinaryComparatorFactory[] getBTreeComparatorFactories();

    public ITypeTrait[] getBTreeTypeTraits();

    public IBTreeInteriorFrameFactory getBTreeInteriorFactory();

    public IBTreeLeafFrameFactory getBTreeLeafFactory();

    public IStorageManagerInterface getStorageManager();

    public IIndexRegistryProvider<BTree> getBTreeRegistryProvider();

    public RecordDescriptor getRecordDescriptor();
}
