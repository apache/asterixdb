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
package org.apache.asterix.external.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.asterix.om.util.AsterixClusterProperties;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint.PartitionConstraintType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.std.file.FileSplit;

public class FeedUtils {
    private static String prepareDataverseFeedName(String dataverseName, String feedName) {
        return dataverseName + File.separator + feedName;
    }

    public static FileSplit[] splitsForAdapter(String dataverseName, String feedName,
            AlgebricksPartitionConstraint partitionConstraints) throws Exception {
        File relPathFile = new File(prepareDataverseFeedName(dataverseName, feedName));
        if (partitionConstraints.getPartitionConstraintType() == PartitionConstraintType.COUNT) {
            throw new AlgebricksException("Can't create file splits for adapter with count partitioning constraints");
        }
        String[] locations = ((AlgebricksAbsolutePartitionConstraint) partitionConstraints).getLocations();
        List<FileSplit> splits = new ArrayList<FileSplit>();
        String storageDirName = AsterixClusterProperties.INSTANCE.getStorageDirectoryName();
        int i = 0;
        for (String nd : locations) {
            // Always get the first partition
            ClusterPartition nodePartition = AsterixClusterProperties.INSTANCE.getNodePartitions(nd)[0];
            String storagePartitionPath = StoragePathUtil.prepareStoragePartitionPath(storageDirName,
                    nodePartition.getPartitionId());
            // format: 'storage dir name'/partition_#/dataverse/feed/adapter_#
            File f = new File(storagePartitionPath + File.separator + relPathFile + File.separator
                    + StoragePathUtil.ADAPTER_INSTANCE_PREFIX + i);
            splits.add(StoragePathUtil.getFileSplitForClusterPartition(nodePartition, f));
            i++;
        }
        return splits.toArray(new FileSplit[] {});
    }

    public static FileReference getAbsoluteFileRef(String relativePath, int ioDeviceId, IIOManager ioManager) {
        return ioManager.getAbsoluteFileRef(ioDeviceId, relativePath);
    }

}
