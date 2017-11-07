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

package org.apache.asterix.test.ioopcallbacks;

import org.apache.asterix.common.ioopcallbacks.LSMRTreeIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.junit.Assert;
import org.mockito.Mockito;

import junit.framework.TestCase;

public class LSMRTreeIOOperationCallbackTest extends TestCase {

    public void testNormalSequence() {
        try {
            ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
            Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
            LSMRTreeIOOperationCallback callback = new LSMRTreeIOOperationCallback(mockIndex);

            //request to flush first component
            callback.updateLastLSN(1);
            callback.beforeOperation(LSMIOOperationType.FLUSH);

            //request to flush second component
            callback.updateLastLSN(2);
            callback.beforeOperation(LSMIOOperationType.FLUSH);

            Assert.assertEquals(1, callback.getComponentLSN(null));
            callback.afterFinalize(LSMIOOperationType.FLUSH, Mockito.mock(ILSMDiskComponent.class));

            Assert.assertEquals(2, callback.getComponentLSN(null));
            callback.afterFinalize(LSMIOOperationType.FLUSH, Mockito.mock(ILSMDiskComponent.class));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    public void testOverWrittenLSN() {
        try {
            ILSMIndex mockIndex = Mockito.mock(ILSMIndex.class);
            Mockito.when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);
            LSMRTreeIOOperationCallback callback = new LSMRTreeIOOperationCallback(mockIndex);

            //request to flush first component
            callback.updateLastLSN(1);
            callback.beforeOperation(LSMIOOperationType.FLUSH);

            //request to flush second component
            callback.updateLastLSN(2);
            callback.beforeOperation(LSMIOOperationType.FLUSH);

            //request to flush first component again
            //this call should fail
            callback.updateLastLSN(3);
            //there is no corresponding beforeOperation, since the first component is being flush
            //the scheduleFlush request would fail this time

            Assert.assertEquals(1, callback.getComponentLSN(null));
            callback.afterFinalize(LSMIOOperationType.FLUSH, Mockito.mock(ILSMDiskComponent.class));

            Assert.assertEquals(2, callback.getComponentLSN(null));
            callback.afterFinalize(LSMIOOperationType.FLUSH, Mockito.mock(ILSMDiskComponent.class));
        } catch (Exception e) {
            Assert.fail();
        }
    }

}
