/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.am.common;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;

public abstract class AbstractIndexLifecycleTest {

    protected IIndex index;

    protected abstract boolean persistentStateExists() throws Exception;

    protected abstract boolean isEmptyIndex() throws Exception;

    protected abstract void performInsertions() throws Exception;

    protected abstract void checkInsertions() throws Exception;

    protected abstract void clearCheckableInsertions() throws Exception;

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
        Assert.assertTrue(isEmptyIndex());

        // Insert some stuff
        performInsertions();
        checkInsertions();

        // Check that the inserted stuff isn't there
        clearCheckableInsertions();
        index.clear();
        Assert.assertTrue(isEmptyIndex());

        // Insert more stuff
        performInsertions();

        // Double close is valid
        index.deactivate();

        // Check that the inserted stuff is still there
        index.activate();
        checkInsertions();
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

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest5() throws Exception {
        index.create();
        index.activate();
        index.activate();
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest6() throws Exception {
        index.create();
        index.activate();
        index.deactivate();
        index.deactivate();
    }
}
