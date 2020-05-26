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
package org.apache.asterix.common.storage;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.asterix.common.utils.StorageConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;

public class ResourceReference {

    protected String root;
    protected String partition;
    protected String dataverse; // == DataverseName.getCanonicalForm()
    protected String dataset;
    protected String rebalance;
    protected String index;
    protected String name;

    protected ResourceReference() {
    }

    public static ResourceReference ofIndex(String indexPath) {
        return of(new File(indexPath, StorageConstants.METADATA_FILE_NAME).toString());
    }

    public static ResourceReference of(String localResourcePath) {
        ResourceReference lrr = new ResourceReference();
        parse(lrr, localResourcePath);
        return lrr;
    }

    public String getPartition() {
        return partition;
    }

    public String getDataverse() { //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
        return dataverse;
    }

    public String getDataset() {
        return dataset;
    }

    public String getRebalance() {
        return rebalance;
    }

    public String getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public Path getRelativePath() {
        return Paths.get(root, partition, dataverse, dataset, rebalance, index);
    }

    public ResourceReference getDatasetReference() {
        return ResourceReference
                .ofIndex(Paths.get(root, partition, dataverse, dataset, rebalance, dataset).toFile().getPath());
    }

    public Path getFileRelativePath() {
        return Paths.get(root, partition, dataverse, dataset, rebalance, index, name);
    }

    protected static void parse(ResourceReference ref, String path) {
        // format: root/partition/dataverse/dataset/rebalanceCount/index/fileName
        final String[] tokens = StringUtils.split(path, File.separatorChar);
        if (tokens.length < 6) {
            throw new IllegalStateException("Unrecognized path structure: " + path);
        }
        int offset = tokens.length;
        ref.name = tokens[--offset];
        ref.index = tokens[--offset];
        ref.rebalance = tokens[--offset];
        ref.dataset = tokens[--offset];
        ref.dataverse = tokens[--offset]; //TODO(MULTI_PART_DATAVERSE_NAME):REVISIT
        ref.partition = tokens[--offset];
        ref.root = tokens[--offset];
    }

    public int getPartitionNum() {
        return Integer.parseInt(partition.substring(StorageConstants.PARTITION_DIR_PREFIX.length()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ResourceReference) {
            ResourceReference that = (ResourceReference) o;
            return getRelativePath().toString().equals(that.getRelativePath().toString());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getRelativePath().toString().hashCode();
    }

    @Override
    public String toString() {
        return getRelativePath().toString();
    }

    /**
     * Gets a component sequence based on its unique timestamp.
     * e.g. a component file 1_3_b
     * will return a component sequence 1_3
     *
     * @param componentFile any component file
     * @return The component sequence
     */
    public static String getComponentSequence(String componentFile) {
        final ResourceReference ref = of(componentFile);
        return IndexComponentFileReference.of(ref.getName()).getSequence();
    }
}
