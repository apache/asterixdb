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

import org.apache.asterix.common.utils.StoragePathUtil;

public class ResourceReference {

    protected String root;
    protected String partition;
    protected String dataverse;
    protected String dataset;
    protected String rebalance;
    protected String index;
    protected String name;

    protected ResourceReference() {
    }

    public static ResourceReference of(String localResourcePath) {
        ResourceReference lrr = new ResourceReference();
        parse(lrr, localResourcePath);
        return lrr;
    }

    public String getPartition() {
        return partition;
    }

    public String getDataverse() {
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

    protected static void parse(ResourceReference ref, String path) {
        // format: root/partition/dataverse/dataset/rebalanceCount/index/fileName
        final String[] tokens = path.split(File.separator);
        if (tokens.length < 6) {
            throw new IllegalStateException("Unrecognized path structure: " + path);
        }
        int offset = tokens.length;
        ref.name = tokens[--offset];
        ref.index = tokens[--offset];
        ref.rebalance = tokens[--offset];
        ref.dataset = tokens[--offset];
        ref.dataverse = tokens[--offset];
        ref.partition = tokens[--offset];
        ref.root = tokens[--offset];
    }

    protected static void parseLegacyPath(ResourceReference ref, String path) {
        // old format: root/partition/dataverse/datasetName_idx_IndexName/fileName
        final String[] tokens = path.split(File.separator);
        if (tokens.length < 4) {
            throw new IllegalStateException("Unrecognized legacy path structure: " + path);
        }
        int offset = tokens.length;
        ref.name = tokens[--offset];
        // split combined dataset/index name
        final String[] indexTokens = tokens[--offset].split(StoragePathUtil.DATASET_INDEX_NAME_SEPARATOR);
        if (indexTokens.length != 2) {
            throw new IllegalStateException("Unrecognized legacy path structure: " + path);
        }
        ref.dataset = indexTokens[0];
        ref.index = indexTokens[1];
        ref.dataverse = tokens[--offset];
        ref.partition = tokens[--offset];
        ref.root = tokens[--offset];
        ref.rebalance = String.valueOf(0);
    }
}
