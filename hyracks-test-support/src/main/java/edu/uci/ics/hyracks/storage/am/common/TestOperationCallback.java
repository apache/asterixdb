package edu.uci.ics.hyracks.storage.am.common;

import java.util.Random;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;

public enum TestOperationCallback implements ISearchOperationCallback, IModificationOperationCallback {
    INSTANCE;

    private static final int RANDOM_SEED = 50;
    private final Random random = new Random();

    private TestOperationCallback() {
        random.setSeed(RANDOM_SEED);
    }

    @Override
    public boolean proceed(ITupleReference tuple) {
        // Fail ~10% of the time
        int i = random.nextInt(100);
        if (i < 10) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void reconcile(ITupleReference tuple) {
        // Do nothing.
    }

    @Override
    public void before(ITupleReference tuple) {
        // Do nothing.        
    }

    @Override
    public void commence(ITupleReference tuple) {
        // Do nothing.        
    }

    @Override
    public void found(ITupleReference tuple) {
        // Do nothing.        
    }

}
