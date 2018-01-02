/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.storage.am.common;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.IoUtil;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.util.Log4j2Monitor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class AbstractIndexLifecycleTest {

    protected IIndex index;

    protected abstract boolean persistentStateExists() throws Exception;

    protected abstract boolean isEmptyIndex() throws Exception;

    protected abstract void performInsertions() throws Exception;

    protected abstract void checkInsertions() throws Exception;

    protected abstract void clearCheckableInsertions() throws Exception;

    @BeforeClass
    public static void startLogsMonitor() {
        Configurator.setLevel("org.apache.hyracks", Level.INFO);
        Log4j2Monitor.start();
    }

    @Before
    public abstract void setup() throws Exception;

    @After
    public abstract void tearDown() throws Exception;

    @Test
    public void validSequenceTest() throws Exception {
        Log4j2Monitor.reset();
        // Double create is invalid
        index.create();
        Assert.assertTrue(persistentStateExists());
        boolean exceptionCaught = false;
        try {
            index.create();
        } catch (Exception e) {
            exceptionCaught = true;
        }
        Assert.assertTrue(exceptionCaught);
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
        index.deactivate();

        // Check that the inserted stuff is still there
        index.activate();
        checkInsertions();
        index.deactivate();

        // Double destroy is idempotent but should log NoSuchFileException
        index.destroy();
        Assert.assertFalse(persistentStateExists());
        index.destroy();
        Assert.assertTrue(Log4j2Monitor.count(IoUtil.FILE_NOT_FOUND_MSG) > 0);
        Assert.assertFalse(persistentStateExists());
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest1() throws Exception {
        index.create();
        index.activate();
        try {
            index.create();
        } finally {
            index.deactivate();
            index.destroy();
        }
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest2() throws Exception {
        index.create();
        index.activate();
        try {
            index.destroy();
        } finally {
            index.deactivate();
            index.destroy();
        }
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest3() throws Exception {
        index.create();
        try {
            index.clear();
        } finally {
            index.destroy();
        }
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest4() throws Exception {
        index.clear();
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest5() throws Exception {
        index.create();
        index.activate();
        try {
            index.activate();
        } finally {
            index.deactivate();
            index.destroy();
        }
    }

    @Test(expected = HyracksDataException.class)
    public void invalidSequenceTest6() throws Exception {
        index.create();
        index.activate();
        index.deactivate();
        try {
            index.deactivate();
        } finally {
            index.destroy();
        }
    }
}
