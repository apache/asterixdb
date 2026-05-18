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
package org.apache.hyracks.storage.am.lsm.vector.util;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.BulkLoadRecordFormat;
import org.junit.Test;

public abstract class VectorIndexTestDriver {

    protected abstract void runTest(ISerializerDeserializer[] centroidSerdes,
            ISerializerDeserializer[] dataRecordSerdes, List<ITupleReference> centroids,
            List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster, int vectorDimension,
            List<List<ITupleReference>> leafRecords) throws Exception;

    @Test
    public void threeDimensionThreeLevels() throws Exception {
        VectorTestStructure dataset = VectorTestStructure.threeDim3Level();

        runTest(dataset.getCentroidSerdes(), dataset.getDataRecordSerdes(BulkLoadRecordFormat.NAIVE),
                dataset.buildCentroidTuples(), dataset.getNumClustersPerLevel(), dataset.getCentroidsPerCluster(),
                dataset.getVectorDimension(), dataset.generateBulkLoadRecords(BulkLoadRecordFormat.NAIVE, 100));
    }
}
