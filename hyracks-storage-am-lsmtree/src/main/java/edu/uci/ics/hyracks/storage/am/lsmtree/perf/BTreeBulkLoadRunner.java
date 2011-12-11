package edu.uci.ics.hyracks.storage.am.lsmtree.perf;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.lsmtree.datagen.TupleBatch;

public class BTreeBulkLoadRunner extends BTreeRunner {

    protected final float fillFactor;
    
    public BTreeBulkLoadRunner(int numBatches, int pageSize, int numPages, ITypeTraits[] typeTraits, MultiComparator cmp, float fillFactor)
            throws HyracksDataException, BTreeException {
        super(numBatches, pageSize, numPages, typeTraits, cmp);
        this.fillFactor = fillFactor;
    }

    @Override
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception {
        btree.create(btreeFileId);
        long start = System.currentTimeMillis();
        IIndexBulkLoadContext bulkLoadCtx = btree.beginBulkLoad(1.0f);
        for (int i = 0; i < numBatches; i++) {
            TupleBatch batch = dataGen.tupleBatchQueue.take();
            for (int j = 0; j < batch.size(); j++) {
                btree.bulkLoadAddTuple(batch.get(j), bulkLoadCtx);    
            }
        }
        btree.endBulkLoad(bulkLoadCtx);
        long end = System.currentTimeMillis();
        long time = end - start;
        return time;
    }
}
