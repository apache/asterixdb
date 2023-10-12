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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataUtil;
import org.apache.asterix.common.utils.StorageConstants;
import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.storage.am.lsm.common.impls.IndexComponentFileReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceReference {

    private static final Logger LOGGER = LogManager.getLogger();
    protected final String root;
    protected final String partition;
    protected final String database;
    protected final DataverseName dataverse;
    protected final String dataset;
    protected final String rebalance;
    protected final String index;
    protected final String name;
    private final Path relativePath;

    protected ResourceReference(String path) {
        // format: root/partition/dataverse/dataset/rebalanceCount/index/fileName
        // format: root/partition/dataverse_p1[/^dataverse_p2[/^dataverse_p3...]]/dataset/rebalanceCount/index/fileName
        final String[] tokens = StringUtils.split(path, File.separatorChar);
        if (tokens.length < 6) {
            throw new IllegalStateException("Unrecognized path structure: " + path);
        }
        int offset = tokens.length;
        name = tokens[--offset];
        index = tokens[--offset];
        rebalance = tokens[--offset];
        dataset = tokens[--offset];
        List<String> dvParts = new ArrayList<>();
        String dvPart = tokens[--offset];
        while (dvPart.codePointAt(0) == StoragePathUtil.DATAVERSE_CONTINUATION_MARKER) {
            dvParts.add(dvPart.substring(1));
            dvPart = tokens[--offset];
        }
        String probablyPartition = tokens[--offset];
        if (dvParts.isEmpty()) {
            // root/partition/dataverse/dataset/rebalanceCount/index/fileName
            // root/partition/database?/dataverse/dataset/rebalanceCount/index/fileName
            try {
                dataverse = DataverseName.createSinglePartName(dvPart);
            } catch (AsterixException e) {
                throw new IllegalArgumentException("unable to parse path: '" + path + "'!", e);
            }
            if (!probablyPartition.startsWith(StorageConstants.PARTITION_DIR_PREFIX)) {
                database = probablyPartition;
                partition = tokens[--offset];
            } else {
                database = MetadataUtil.databaseFor(dataverse);
                partition = probablyPartition;
            }
            root = tokens[--offset];
        } else if (probablyPartition.startsWith(StorageConstants.PARTITION_DIR_PREFIX)) {
            // root/partition/dataverse_p1/^dataverse_p2/.../^dataverse_pn/dataset/rebalanceCount/index/fileName
            dvParts.add(dvPart);
            Collections.reverse(dvParts);
            try {
                dataverse = DataverseName.create(dvParts);
            } catch (AsterixException e) {
                throw new IllegalArgumentException("unable to parse path: '" + path + "'!", e);
            }
            database = MetadataUtil.databaseFor(dataverse);
            partition = probablyPartition;
            root = tokens[--offset];
        } else if (dvPart.startsWith(StorageConstants.PARTITION_DIR_PREFIX)) {
            // root/partition/dataverse/dataset/rebalanceCount/index/fileName (where dataverse starts with ^)
            if (dvParts.size() != 1) {
                throw new IllegalArgumentException("unable to parse path: '" + path + "'!");
            }
            try {
                dataverse = DataverseName
                        .createSinglePartName(StoragePathUtil.DATAVERSE_CONTINUATION_MARKER + dvParts.get(0));
            } catch (AsterixException e) {
                throw new IllegalArgumentException("unable to parse path: '" + path + "'!", e);
            }
            LOGGER.info("legacy dataverse starting with ^ found: '{}'; this is not supported for new dataverses",
                    dataverse);
            database = MetadataUtil.databaseFor(dataverse);
            partition = dvPart;
            root = probablyPartition;
        } else {
            throw new IllegalArgumentException("unable to parse path: '" + path + "'!");
        }
        relativePath = Paths.get(root, ArrayUtils.subarray(tokens, offset + 1, tokens.length - 1));
    }

    public static ResourceReference ofIndex(String indexPath) {
        return of(new File(indexPath, StorageConstants.METADATA_FILE_NAME).toString());
    }

    public static ResourceReference of(String localResourcePath) {
        return new ResourceReference(localResourcePath);
    }

    public String getPartition() {
        return partition;
    }

    public String getDatabase() {
        return database;
    }

    public DataverseName getDataverse() {
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
        return relativePath;
    }

    public ResourceReference getDatasetReference() {
        return ResourceReference.ofIndex(relativePath.getParent().resolve(dataset).toFile().getPath());
    }

    public boolean isMetadataResource() {
        return getName().equals(StorageConstants.METADATA_FILE_NAME);
    }

    public Path getFileRelativePath() {
        return relativePath.resolve(name);
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
            return getRelativePath().equals(that.getRelativePath());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getRelativePath().hashCode();
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
