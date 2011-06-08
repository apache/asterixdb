package edu.uci.ics.hyracks.storage.am.rtree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexHelperOpenMode;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexOpHelper;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.impls.InteriorFrameSchema;
import edu.uci.ics.hyracks.storage.am.rtree.impls.RTree;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class RTreeOpHelper extends TreeIndexOpHelper {

    protected MultiComparator interiorCmp;

    public RTreeOpHelper(ITreeIndexOperatorDescriptorHelper opDesc, IHyracksStageletContext ctx, int partition,
            IndexHelperOpenMode mode) {
        super(opDesc, ctx, partition, mode);
    }

    public void init() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        IFileMapProvider fileMapProvider = opDesc.getStorageManager().getFileMapProvider(ctx);
        IFileSplitProvider fileSplitProvider = opDesc.getTreeIndexFileSplitProvider();

        FileReference f = fileSplitProvider.getFileSplits()[partition].getLocalFile();
        boolean fileIsMapped = fileMapProvider.isMapped(f);

        switch (mode) {

            case OPEN: {
                if (!fileIsMapped) {
                    throw new HyracksDataException("Trying to open rtree from unmapped file " + f.toString());
                }
            }
                break;

            case CREATE:
            case ENLIST: {
                if (!fileIsMapped) {
                    bufferCache.createFile(f);
                }
            }
                break;

        }

        int fileId = fileMapProvider.lookupFileId(f);
        try {
            bufferCache.openFile(fileId);
        } catch (HyracksDataException e) {
            // revert state of buffer cache since file failed to open
            if (!fileIsMapped) {
                bufferCache.deleteFile(fileId);
            }
            throw e;
        }

        // only set indexFileId member when openFile() succeeds,
        // otherwise deinit() will try to close the file that failed to open
        indexFileId = fileId;

        interiorFrame = opDesc.getTreeIndexInteriorFactory().createFrame();
        leafFrame = opDesc.getTreeIndexLeafFactory().createFrame();

        IndexRegistry<ITreeIndex> treeIndexRegistry = opDesc.getTreeIndexRegistryProvider().getRegistry(ctx);
        treeIndex = treeIndexRegistry.get(indexFileId);
        if (treeIndex == null) {

            // create new tree and register it
            treeIndexRegistry.lock();
            try {
                // check if tree has already been registered by another thread
                treeIndex = treeIndexRegistry.get(indexFileId);
                if (treeIndex == null) {
                    // this thread should create and register the tree

                    IBinaryComparator[] comparators = new IBinaryComparator[opDesc.getTreeIndexComparatorFactories().length];
                    for (int i = 0; i < opDesc.getTreeIndexComparatorFactories().length; i++) {
                        comparators[i] = opDesc.getTreeIndexComparatorFactories()[i].createBinaryComparator();
                    }

                    cmp = new MultiComparator(opDesc.getTreeIndexTypeTraits(), comparators);
                    InteriorFrameSchema interiorFrameSchema = new InteriorFrameSchema(cmp);
                    interiorCmp = interiorFrameSchema.getInteriorCmp();

                    treeIndex = createTreeIndex();
                    if (mode == IndexHelperOpenMode.CREATE) {
                        ITreeIndexMetaDataFrame metaFrame = treeIndex.getFreePageManager().getMetaDataFrameFactory()
                                .createFrame();
                        try {
                            treeIndex.create(indexFileId, leafFrame, metaFrame);
                        } catch (Exception e) {
                            throw new HyracksDataException(e);
                        }
                    }
                    treeIndex.open(indexFileId);
                    treeIndexRegistry.register(indexFileId, treeIndex);
                }
            } finally {
                treeIndexRegistry.unlock();
            }
        }
    }

    public ITreeIndex createTreeIndex() throws HyracksDataException {
        IBufferCache bufferCache = opDesc.getStorageManager().getBufferCache(ctx);
        ITreeIndexMetaDataFrameFactory metaDataFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager freePageManager = new LinkedListFreePageManager(bufferCache, indexFileId, 0,
                metaDataFrameFactory);

        return new RTree(bufferCache, freePageManager, opDesc.getTreeIndexInteriorFactory(),
                opDesc.getTreeIndexLeafFactory(), interiorCmp, cmp);
    }
    
    public ITreeIndexCursor createDiskOrderScanCursor(ITreeIndexFrame leafFrame) throws HyracksDataException {
        throw new HyracksDataException("createDiskOrderScanCursor Operation not implemented.");
    }
}
