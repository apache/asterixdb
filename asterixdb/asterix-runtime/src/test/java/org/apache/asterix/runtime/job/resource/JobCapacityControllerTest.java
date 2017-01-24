/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.asterix.runtime.job.resource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.resource.ClusterCapacity;
import org.apache.hyracks.api.job.resource.IClusterCapacity;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.job.resource.NodeCapacity;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.hyracks.control.cc.scheduler.ResourceManager;
import org.junit.Assert;
import org.junit.Test;

public class JobCapacityControllerTest {

    @Test
    public void test() throws HyracksException {
        IResourceManager resourceManager = makeResourceManagerWithCapacity(4294967296L, 33);
        JobCapacityController capacityController = new JobCapacityController(resourceManager);

        // Verifies the correctness of the allocate method.
        Assert.assertTrue(capacityController.allocate(
                makeJobWithRequiredCapacity(4294967296L, 16)) == IJobCapacityController.JobSubmissionStatus.EXECUTE);
        Assert.assertTrue(capacityController.allocate(
                makeJobWithRequiredCapacity(2147483648L, 16)) == IJobCapacityController.JobSubmissionStatus.QUEUE);
        Assert.assertTrue(capacityController.allocate(
                makeJobWithRequiredCapacity(2147483648L, 32)) == IJobCapacityController.JobSubmissionStatus.QUEUE);

        boolean exceedCapacity = false;
        try {
            capacityController.allocate(makeJobWithRequiredCapacity(2147483648L, 64));
        } catch (HyracksException e) {
            exceedCapacity = e.getErrorCode() == ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY;
        }
        Assert.assertTrue(exceedCapacity);
        Assert.assertTrue(capacityController.allocate(
                makeJobWithRequiredCapacity(4294967296L, 32)) == IJobCapacityController.JobSubmissionStatus.QUEUE);
        exceedCapacity = false;
        try {
            capacityController.allocate(makeJobWithRequiredCapacity(4294967297L, 33));
        } catch (HyracksException e) {
            exceedCapacity = e.getErrorCode() == ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY;
        }
        Assert.assertTrue(exceedCapacity);

        // Verifies that the release method does not leak resource.
        capacityController.release(makeJobWithRequiredCapacity(4294967296L, 16));
        Assert.assertTrue(resourceManager.getCurrentCapacity().getAggregatedMemoryByteSize() == 4294967296L);
        Assert.assertTrue(resourceManager.getCurrentCapacity().getAggregatedCores() == 33);
    }

    private IResourceManager makeResourceManagerWithCapacity(long memorySize, int cores) throws HyracksException {
        IResourceManager resourceManager = new ResourceManager();
        resourceManager.update("node1", new NodeCapacity(memorySize, cores));
        return resourceManager;
    }

    private JobSpecification makeJobWithRequiredCapacity(long memorySize, int cores) {
        // Generates cluster capacity.
        IClusterCapacity clusterCapacity = makeComputationCapacity(memorySize, cores);

        // Generates a job.
        JobSpecification job = mock(JobSpecification.class);
        when(job.getRequiredClusterCapacity()).thenReturn(clusterCapacity);
        return job;
    }

    private IClusterCapacity makeComputationCapacity(long memorySize, int cores) {
        IClusterCapacity clusterCapacity = new ClusterCapacity();
        clusterCapacity.setAggregatedMemoryByteSize(memorySize);
        clusterCapacity.setAggregatedCores(cores);
        return clusterCapacity;
    }

}
