/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.config;

/**
 * Global parameters used for executing access method JUnit tests.
 */
public class AccessMethodTestsConfig {
    // Test params for RTree, LSMRTree and LSMRTreeWithAntiMatterTuples.
    public static final int RTREE_NUM_TUPLES_TO_INSERT = 100;
    public static final int RTREE_NUM_INSERT_ROUNDS = 2;
    public static final int RTREE_NUM_DELETE_ROUNDS = 2;
    public static final int RTREE_MULTITHREAD_NUM_OPERATIONS = 100;
    public static final boolean RTREE_TEST_RSTAR_POLICY = true;
    // Test params for LSMRTree and LSMRTreeWithAntiMatterTuples.
    public static final int LSM_RTREE_BULKLOAD_ROUNDS = 5;
    public static final int LSM_RTREE_MAX_TREES_TO_MERGE = 3;
    public static final boolean LSM_RTREE_TEST_RSTAR_POLICY = false;

    // Test params for BTree, LSMBTree.
    public static final int BTREE_NUM_TUPLES_TO_INSERT = 100;
    public static final int BTREE_NUM_INSERT_ROUNDS = 3;
    public static final int BTREE_NUM_DELETE_ROUNDS = 3;
    public static final int BTREE_NUM_UPDATE_ROUNDS = 3;
    public static final int BTREE_MULTITHREAD_NUM_OPERATIONS = 100;
    // Test params for LSMBTree only.
    public static final int LSM_BTREE_BULKLOAD_ROUNDS = 5;
    public static final int LSM_BTREE_MAX_TREES_TO_MERGE = 10;

    // Mem configuration for RTree.
    public static final int RTREE_PAGE_SIZE = 512;
    public static final int RTREE_NUM_PAGES = 1000;
    public static final int RTREE_MAX_OPEN_FILES = 10;
    public static final int RTREE_HYRACKS_FRAME_SIZE = 128;

    // Mem configuration for LSMRTree and LSMRTreeWithAntiMatterTuples.
    public static final int LSM_RTREE_DISK_PAGE_SIZE = 256;
    public static final int LSM_RTREE_DISK_NUM_PAGES = 1000;
    public static final int LSM_RTREE_DISK_MAX_OPEN_FILES = 2000;
    public static final int LSM_RTREE_MEM_PAGE_SIZE = 256;
    public static final int LSM_RTREE_MEM_NUM_PAGES = 1000;
    public static final int LSM_RTREE_HYRACKS_FRAME_SIZE = 128;

    // Mem configuration for BTree.
    public static final int BTREE_PAGE_SIZE = 256;
    public static final int BTREE_NUM_PAGES = 100;
    public static final int BTREE_MAX_OPEN_FILES = 10;
    public static final int BTREE_HYRACKS_FRAME_SIZE = 128;

    // Mem configuration for LSMBTree.
    public static final int LSM_BTREE_DISK_PAGE_SIZE = 256;
    public static final int LSM_BTREE_DISK_NUM_PAGES = 1000;
    public static final int LSM_BTREE_DISK_MAX_OPEN_FILES = 200;
    public static final int LSM_BTREE_MEM_PAGE_SIZE = 256;
    public static final int LSM_BTREE_MEM_NUM_PAGES = 100;
    public static final int LSM_BTREE_HYRACKS_FRAME_SIZE = 128;

    // Mem configuration for Inverted Index.
    public static final int LSM_INVINDEX_PAGE_SIZE = 32768;
    public static final int LSM_INVINDEX_NUM_PAGES = 100;
    public static final int LSM_INVINDEX_MAX_OPEN_FILES = 10;
    public static final int LSM_INVINDEX_HYRACKS_FRAME_SIZE = 32768;
    
    public static final int LSM_INVINDEX_NUM_BULKLOAD_ROUNDS = 5;
    public static final int LSM_INVINDEX_MAX_TREES_TO_MERGE = 5;
    
}

/* ORIGINAL TEST PARAMETERS: DO NOT EDIT!
// Test params for RTree, LSMRTree and LSMRTreeWithAntiMatterTuples.
public static final int RTREE_NUM_TUPLES_TO_INSERT = 10000;
public static final int RTREE_NUM_INSERT_ROUNDS = 2;
public static final int RTREE_NUM_DELETE_ROUNDS = 2;
public static final int RTREE_MULTITHREAD_NUM_OPERATIONS = 10000;
// Test params for LSMRTree and LSMRTreeWithAntiMatterTuples.
public static final int LSM_RTREE_BULKLOAD_ROUNDS = 5;
public static final int LSM_RTREE_MAX_TREES_TO_MERGE = 3;	

// Test params for BTree, LSMBTree.
public static final int BTREE_NUM_TUPLES_TO_INSERT = 10000;
public static final int BTREE_NUM_INSERT_ROUNDS = 3;
public static final int BTREE_NUM_DELETE_ROUNDS = 3;
public static final int BTREE_NUM_UPDATE_ROUNDS = 3;
public static final int BTREE_MULTITHREAD_NUM_OPERATIONS = 10000;
// Test params for LSMBTree only.
public static final int LSM_BTREE_BULKLOAD_ROUNDS = 5;
public static final int LSM_BTREE_MAX_TREES_TO_MERGE = 10;
	
	
// Mem configuration for RTree.
public static final int RTREE_PAGE_SIZE = 512;
public static final int RTREE_NUM_PAGES = 1000;
public static final int RTREE_MAX_OPEN_FILES = 10;
public static final int RTREE_HYRACKS_FRAME_SIZE = 128;
	
// Mem configuration for LSMRTree and LSMRTreeWithAntiMatterTuples.
public static final int LSM_RTREE_DISK_PAGE_SIZE = 256;
public static final int LSM_RTREE_DISK_NUM_PAGES = 1000;
public static final int LSM_RTREE_DISK_MAX_OPEN_FILES = 2000;
public static final int LSM_RTREE_MEM_PAGE_SIZE = 256;
public static final int LSM_RTREE_MEM_NUM_PAGES = 1000;
public static final int LSM_RTREE_HYRACKS_FRAME_SIZE = 128;
	
// Mem configuration for BTree.
public static final int BTREE_PAGE_SIZE = 256;
public static final int BTREE_NUM_PAGES = 100;
public static final int BTREE_MAX_OPEN_FILES = 10;
public static final int BTREE_HYRACKS_FRAME_SIZE = 128;
	
// Mem configuration for LSMBTree.
public static final int LSM_BTREE_DISK_PAGE_SIZE = 256;
public static final int LSM_BTREE_DISK_NUM_PAGES = 1000;
public static final int LSM_BTREE_DISK_MAX_OPEN_FILES = 200;
public static final int LSM_BTREE_MEM_PAGE_SIZE = 256;
public static final int LSM_BTREE_MEM_NUM_PAGES = 100;
public static final int LSM_BTREE_HYRACKS_FRAME_SIZE = 128;

// Mem configuration for Inverted Index.
public static final int INVINDEX_PAGE_SIZE = 32768;
public static final int INVINDEX_NUM_PAGES = 100;
public static final int INVINDEX_MAX_OPEN_FILES = 10;
public static final int INVINDEX_HYRACKS_FRAME_SIZE = 32768;
*/
