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
package org.apache.asterix.metadata.lock;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.LockList;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.hyracks.api.util.SingleThreadEventProcessor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MetadataLockManagerTest {

    //TODO(DB): adapt test for database
    static final int REPREAT_TEST_COUNT = 3;

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[REPREAT_TEST_COUNT][0]);
    }

    private static class Request {
        private enum Statement {
            INDEX,
            MODIFY,
            EXCLUSIVE_MODIFY,
            EXCLUSIVE_MODIFY_UPGRADE_DOWNGRADE,
            EXCLUSIVE_MODIFY_UPGRADE,
        }

        private final Statement statement;
        private final String database;
        private final DataverseName dataverseName;
        private final String datasetName;
        private boolean done;
        private int step = 0;

        public Request(Statement statement, String database, DataverseName dataverseName, String datasetName) {
            this.database = database;
            this.statement = statement;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            done = false;
        }

        Statement statement() {
            return statement;
        }

        String database() {
            return database;
        }

        DataverseName dataverse() {
            return dataverseName;
        }

        String dataset() {
            return datasetName;
        }

        synchronized void complete() {
            done = true;
            notifyAll();
        }

        synchronized void await() throws InterruptedException {
            while (!done) {
                wait();
            }
        }

        synchronized void step() {
            step++;
            notifyAll();
        }

        synchronized int getSteps() {
            return step;
        }

        synchronized void await(int step) throws InterruptedException {
            while (this.step < step) {
                wait();
            }
        }
    }

    public class User extends SingleThreadEventProcessor<Request> {

        private MetadataLockManager lockManager;
        private Semaphore step = new Semaphore(0);
        private final LockList locks = new LockList();

        public User(String username, MetadataLockManager lockManager) {
            super(username);
            this.lockManager = lockManager;
        }

        public void step() {
            step.release();
        }

        @Override
        protected void handle(Request req) throws Exception {
            try {
                step.acquire();
                switch (req.statement()) {
                    case INDEX:
                        lockManager.acquireDatasetCreateIndexLock(locks, req.database(), req.dataverse(),
                                req.dataset());
                        break;
                    case MODIFY:
                        lockManager.acquireDatasetModifyLock(locks, req.database(), req.dataverse(), req.dataset());
                        break;
                    case EXCLUSIVE_MODIFY:
                        lockManager.acquireDatasetExclusiveModificationLock(locks, req.database(), req.dataverse(),
                                req.dataset());
                        break;
                    case EXCLUSIVE_MODIFY_UPGRADE:
                        lockManager.acquireDatasetExclusiveModificationLock(locks, req.database(), req.dataverse(),
                                req.dataset());
                        req.step();
                        step.acquire();
                        lockManager.upgradeDatasetLockToWrite(locks, req.database(), req.dataverse(), req.dataset());
                        break;
                    case EXCLUSIVE_MODIFY_UPGRADE_DOWNGRADE:
                        lockManager.acquireDatasetExclusiveModificationLock(locks, req.database(), req.dataverse(),
                                req.dataset());
                        req.step();
                        step.acquire();
                        lockManager.upgradeDatasetLockToWrite(locks, req.database(), req.dataverse(), req.dataset());
                        req.step();
                        step.acquire();
                        lockManager.downgradeDatasetLockToExclusiveModify(locks, req.database(), req.dataverse(),
                                req.dataset());
                        break;
                    default:
                        break;
                }
                req.step();
                step.acquire();
            } finally {
                locks.reset();
                req.step();
                req.complete();
            }
        }

    }

    @Test
    public void testDatasetLockMultipleIndexBuildsSingleModifier() throws Exception {
        MetadataLockManager lockManager = new MetadataLockManager();
        DataverseName dataverseName = DataverseName.createSinglePartName("Dataverse");
        String database = MetadataUtil.databaseFor(dataverseName);
        String datasetName = "Dataset";
        User till = new User("till", lockManager);
        Request tReq = new Request(Request.Statement.INDEX, database, dataverseName, datasetName);
        User dmitry = new User("dmitry", lockManager);
        Request dReq = new Request(Request.Statement.INDEX, database, dataverseName, datasetName);
        User mike = new User("mike", lockManager);
        Request mReq = new Request(Request.Statement.MODIFY, database, dataverseName, datasetName);
        // Till builds an index
        till.add(tReq);
        // Dmitry builds an index
        dmitry.add(dReq);
        // Mike modifies
        mike.add(mReq);
        // Till starts
        till.step();
        // Ensure lock acquired
        tReq.await(1);
        // Dmitry starts
        dmitry.step();
        // Ensure lock acquired
        dReq.await(1);
        // Mike starts and is allowed to go all the way
        mike.step();
        mike.step();
        // Ensure that Mike still could not acquire locks
        Assert.assertEquals(0, mReq.getSteps());
        // Till finishes first
        till.step();
        // Ensure the request has been completed and lock has been released
        tReq.await();
        // Ensure that Mike still could not acquire locks
        Assert.assertEquals(0, mReq.getSteps());
        // Dmitry finishes second
        dmitry.step();
        // Ensure the request has been completed and lock has been released
        dReq.await();
        // Ensure that Mike could proceed and request has been completed
        mReq.await();
        // Stop users
        till.stop();
        dmitry.stop();
        mike.stop();
    }

    @Test
    public void testDatasetLockMultipleModifiersSingleIndexBuilder() throws Exception {
        MetadataLockManager lockManager = new MetadataLockManager();
        DataverseName dataverseName = DataverseName.createSinglePartName("Dataverse");
        String database = MetadataUtil.databaseFor(dataverseName);
        String datasetName = "Dataset";
        User till = new User("till", lockManager);
        Request tReq = new Request(Request.Statement.MODIFY, database, dataverseName, datasetName);
        User dmitry = new User("dmitry", lockManager);
        Request dReq = new Request(Request.Statement.MODIFY, database, dataverseName, datasetName);
        User mike = new User("mike", lockManager);
        Request mReq = new Request(Request.Statement.INDEX, database, dataverseName, datasetName);
        // Till modifies
        till.add(tReq);
        // Dmitry modifies
        dmitry.add(dReq);
        // Mike builds an index
        mike.add(mReq);
        // Till starts
        till.step();
        // Ensure lock acquired
        tReq.await(1);
        // Dmitry starts
        dmitry.step();
        // Ensure lock acquired
        dReq.await(1);
        // Mike starts and is allowed to go all the way
        mike.step();
        mike.step();
        // Ensure that Mike still could not acquire locks
        Assert.assertEquals(0, mReq.getSteps());
        // Till finishes first
        till.step();
        // Ensure the request has been completed and lock has been released
        tReq.await();
        // Ensure that Mike still could not acquire locks
        Assert.assertEquals(0, mReq.getSteps());
        // Dmitry finishes second
        dmitry.step();
        // Ensure the request has been completed and lock has been released
        dReq.await();
        // Ensure that Mike could proceed and request has been completed
        mReq.await();
        // Stop users
        till.stop();
        dmitry.stop();
        mike.stop();
    }

    @Test
    public void testDatasetLockMultipleModifiersSingleExclusiveModifier() throws Exception {
        MetadataLockManager lockManager = new MetadataLockManager();
        DataverseName dataverseName = DataverseName.createSinglePartName("Dataverse");
        String database = MetadataUtil.databaseFor(dataverseName);
        String datasetName = "Dataset";
        User till = new User("till", lockManager);
        Request tReq = new Request(Request.Statement.MODIFY, database, dataverseName, datasetName);
        User dmitry = new User("dmitry", lockManager);
        Request dReq = new Request(Request.Statement.MODIFY, database, dataverseName, datasetName);
        User mike = new User("mike", lockManager);
        Request mReq = new Request(Request.Statement.EXCLUSIVE_MODIFY, database, dataverseName, datasetName);
        // Till starts
        till.add(tReq);
        till.step();
        // Ensure lock is acquired
        tReq.await(1);
        // Mike starts
        mike.add(mReq);
        mike.step();
        // Sleep for 1s for now as there is no way to find out user has submitted the exclusive lock request
        Thread.sleep(1000);
        // Ensure that Mike didn't get the lock
        Assert.assertEquals(0, mReq.getSteps());
        // Dmitry starts
        dmitry.add(dReq);
        dmitry.step();
        // Ensure that Dmitry didn't get the lock
        Assert.assertEquals(0, dReq.getSteps());
        // Till proceeds
        till.step();
        // Ensure the request has been completed and lock has been released
        tReq.await();
        // Ensure that Mike got the lock
        mReq.await(1);
        // Till submits another request
        tReq = new Request(Request.Statement.MODIFY, database, dataverseName, datasetName);
        till.add(tReq);
        till.step();
        // Ensure that Till didn't get the lock
        Assert.assertEquals(0, tReq.getSteps());
        // Ensure that Dmitry didn't get the lock
        Assert.assertEquals(0, dReq.getSteps());
        // Mike completes
        mike.step();
        mReq.await();
        // Ensure that  both Till and Dmitry got the lock
        tReq.await(1);
        dReq.await(1);
        till.step();
        dmitry.step();
        // Ensure that  both Till and Dmitry complete
        tReq.await();
        dReq.await();
        // Stop users
        till.stop();
        dmitry.stop();
        mike.stop();
    }

}
