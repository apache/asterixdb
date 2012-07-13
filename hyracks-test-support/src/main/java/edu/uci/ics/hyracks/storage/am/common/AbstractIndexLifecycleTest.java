package edu.uci.ics.hyracks.storage.am.common;

import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;

public abstract class AbstractIndexLifecycleTest {

    protected IIndex index;

    protected TreeIndexTestUtils titu;

    protected ITreeIndexTestContext<? extends CheckTuple> testCtx;

    protected abstract boolean persistentStateExists() throws Exception;

    protected abstract boolean isEmptyIndex() throws Exception;

    protected abstract Random getRandom();

    @Before
    public abstract void setup() throws Exception;

    @After
    public abstract void tearDown() throws Exception;

    @Test
    public void validSequenceTest() throws Exception {
        // Double create is valid
        index.create();
        Assert.assertTrue(persistentStateExists());
        index.create();
        Assert.assertTrue(persistentStateExists());

        // Double open is valid
        index.activate();
        index.activate();
        Assert.assertTrue(isEmptyIndex());

        // insert some stuff
        titu.insertIntTuples(testCtx, 10, getRandom());

        index.clear();
        testCtx.getCheckTuples().clear();
        Assert.assertTrue(isEmptyIndex());

        // check that the inserted stuff is not there
        titu.checkScan(testCtx);

        // insert more stuff
        titu.insertIntTuples(testCtx, 10, getRandom());

        // Double close is valid
        index.deactivate();
        index.deactivate();

        // Reopen and reclose is valid
        index.activate();

        // make sure the stuff we inserted is still there
        // TODO: re-enable when LSM index's close() is implemented properly
        // titu.checkScan(testCtx);

        index.deactivate();

        // Double destroy is valid
        index.destroy();
        Assert.assertFalse(persistentStateExists());
        index.destroy();
        Assert.assertFalse(persistentStateExists());
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest1() throws Exception {
        index.create();
        index.activate();
        index.create();
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest2() throws Exception {
        index.create();
        index.activate();
        index.destroy();
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest3() throws Exception {
        index.create();
        index.clear();
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest4() throws Exception {
        index.clear();
    }
}
