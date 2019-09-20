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
package org.apache.asterix.common.utils;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;
import org.apache.hyracks.storage.am.lsm.common.impls.ConcurrentMergePolicyFactory;

/**
 * A static class that stores storage constants
 */
public class StorageConstants {

    public static final String STORAGE_ROOT_DIR_NAME = "storage";
    public static final String PARTITION_DIR_PREFIX = "partition_";
    /**
     * Any file that shares the same directory as the LSM index files must
     * begin with ".". Otherwise {@link AbstractLSMIndexFileManager} will try to
     * use them as index files.
     */
    public static final String INDEX_CHECKPOINT_FILE_PREFIX = ".idx_checkpoint_";
    public static final String METADATA_FILE_NAME = ".metadata";
    public static final String MASK_FILE_PREFIX = ".mask_";
    public static final String COMPONENT_MASK_FILE_PREFIX = MASK_FILE_PREFIX + "C_";
    public static final float DEFAULT_TREE_FILL_FACTOR = 1.00f;
    public static final String DEFAULT_COMPACTION_POLICY_NAME = ConcurrentMergePolicyFactory.NAME;
    public static final String DEFAULT_FILTERED_DATASET_COMPACTION_POLICY_NAME = "correlated-prefix";
    public static final Map<String, String> DEFAULT_COMPACTION_POLICY_PROPERTIES;

    /**
     * The storage version of AsterixDB related artifacts (e.g. log files, checkpoint files, etc..).
     */
    private static final int LOCAL_STORAGE_VERSION = 5;

    /**
     * The storage version of AsterixDB stack.
     */
    public static final int VERSION = LOCAL_STORAGE_VERSION + ITreeIndexFrame.Constants.VERSION;

    static {
        DEFAULT_COMPACTION_POLICY_PROPERTIES = new LinkedHashMap<>();
        DEFAULT_COMPACTION_POLICY_PROPERTIES.put(ConcurrentMergePolicyFactory.MAX_COMPONENT_COUNT, "30");
        DEFAULT_COMPACTION_POLICY_PROPERTIES.put(ConcurrentMergePolicyFactory.MIN_MERGE_COMPONENT_COUNT, "3");
        DEFAULT_COMPACTION_POLICY_PROPERTIES.put(ConcurrentMergePolicyFactory.MAX_MERGE_COMPONENT_COUNT, "10");
        DEFAULT_COMPACTION_POLICY_PROPERTIES.put(ConcurrentMergePolicyFactory.SIZE_RATIO, "1.2");
    }

    private StorageConstants() {
    }
}
