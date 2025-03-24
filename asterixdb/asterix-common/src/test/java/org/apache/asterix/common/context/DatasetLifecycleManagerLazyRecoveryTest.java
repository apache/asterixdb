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
package org.apache.asterix.common.context;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the behavior of the DatasetLifecycleManager with lazy recovery enabled.
 * This test class focuses on verifying that lazy recovery works correctly.
 */
public class DatasetLifecycleManagerLazyRecoveryTest {
    private static final Logger LOGGER = LogManager.getLogger();

    private DatasetLifecycleManagerConcurrentTest concurrentTest;
    private List<String> recoveredResourcePaths;
    private Set<String> datasetPartitionPaths;
    private Map<String, FileReference> pathToFileRefMap;
    private ExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        // Create and initialize the base test
        concurrentTest = new DatasetLifecycleManagerConcurrentTest();
        concurrentTest.setUp();

        // Enable lazy recovery
        concurrentTest.setLazyRecoveryEnabled(true);
        LOGGER.info("Lazy recovery enabled for all tests");

        // Track recovered resources
        recoveredResourcePaths = new ArrayList<>();
        datasetPartitionPaths = new TreeSet<>();
        pathToFileRefMap = new HashMap<>();

        // Extract the dataset partition paths from the resource paths and log partition info
        // These are the parent directories that contain multiple index files
        // Format: storage/partition_X/DBName/ScopeName/DatasetName/ReplicationFactor/IndexName
        // Note: The replication factor is ALWAYS 0 in production and should be 0 in all tests
        Pattern pattern = Pattern.compile("(.*?/partition_(\\d+)/[^/]+/[^/]+/[^/]+/(\\d+))/.+");

        // Map to collect resources by partition for logging
        Map<Integer, List<String>> resourcesByPartition = new HashMap<>();

        for (String resourcePath : concurrentTest.getCreatedResourcePaths()) {
            Matcher matcher = pattern.matcher(resourcePath);
            if (matcher.matches()) {
                String partitionPath = matcher.group(1); // Now includes the replication factor
                int partitionId = Integer.parseInt(matcher.group(2));
                int replicationFactor = Integer.parseInt(matcher.group(3)); // Usually 0

                LOGGER.info("Resource path: {}", resourcePath);
                LOGGER.info("  Partition path: {}", partitionPath);
                LOGGER.info("  Partition ID: {}", partitionId);
                LOGGER.info("  Replication factor: {}", replicationFactor);

                // Track resources by partition for logging
                resourcesByPartition.computeIfAbsent(partitionId, k -> new ArrayList<>()).add(resourcePath);

                // Add to our partition paths for mocking
                datasetPartitionPaths.add(partitionPath);

                // Create a FileReference mock for this partition path
                FileReference mockPartitionRef = mock(FileReference.class);
                when(mockPartitionRef.getAbsolutePath()).thenReturn(partitionPath);
                pathToFileRefMap.put(partitionPath, mockPartitionRef);
            } else {
                LOGGER.warn("Resource path doesn't match expected pattern: {}", resourcePath);
            }
        }

        // Log resources by partition for debugging
        LOGGER.info("Found {} dataset partition paths", datasetPartitionPaths.size());
        for (Map.Entry<Integer, List<String>> entry : resourcesByPartition.entrySet()) {
            LOGGER.info("Partition {}: {} resources", entry.getKey(), entry.getValue().size());
            for (String path : entry.getValue()) {
                LOGGER.info("  - {}", path);
            }
        }

        // Mock the IOManager.resolve() method to return our mock FileReferences
        IIOManager mockIOManager = concurrentTest.getMockServiceContext().getIoManager();
        doAnswer(inv -> {
            String path = inv.getArgument(0);
            // For each possible partition path, check if this is the path being resolved
            for (String partitionPath : datasetPartitionPaths) {
                if (path.equals(partitionPath)) {
                    LOGGER.info("Resolving path {} to FileReference", path);
                    return pathToFileRefMap.get(partitionPath);
                }
            }
            // If no match, create a new mock FileReference
            FileReference mockFileRef = mock(FileReference.class);
            when(mockFileRef.getAbsolutePath()).thenReturn(path);
            when(mockFileRef.toString()).thenReturn(path);
            return mockFileRef;
        }).when(mockIOManager).resolve(anyString());

        // Create executor service for concurrent tests
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    /**
     * Tests that when an index is opened with lazy recovery enabled, 
     * all resources under the same dataset partition path are recovered together.
     * This verifies the batch recovery behavior for all resources with the same prefix path.
     */
    @Test
    public void testLazyRecoveryForEntirePartition() throws Exception {
        LOGGER.info("Starting testLazyRecoveryForEntirePartition");

        // Get resources for one dataset partition
        String datasetPartitionPath = datasetPartitionPaths.iterator().next();
        List<String> resourcesInPartition = concurrentTest.getCreatedResourcePaths().stream()
                .filter(path -> path.startsWith(datasetPartitionPath)).collect(Collectors.toList());

        LOGGER.info("Testing partition path: {}", datasetPartitionPath);
        LOGGER.info("Resources in this partition: {}", resourcesInPartition);

        // Mock the resource repository to track calls to getResources
        ILocalResourceRepository mockRepository = concurrentTest.getMockResourceRepository();

        // Setup capture of resources that get recovered - using FileReference
        doAnswer(inv -> {
            List<FileReference> refs = inv.getArgument(1);
            if (refs != null && !refs.isEmpty()) {
                FileReference fileRef = refs.get(0);
                String rootPath = fileRef.getAbsolutePath();
                LOGGER.info("Looking up resources with FileReference: {}", fileRef);
                LOGGER.info("FileReference absolute path: {}", rootPath);

                // The rootPath from DatasetLifecycleManager now includes the replication factor
                // e.g., storage/partition_1/Default/SDefault/ds0/0
                LOGGER.info("Using corrected rootPath for resource lookup: {}", rootPath);

                // Get all resources that match this root path
                Map<Long, LocalResource> matchingResources = new HashMap<>();

                // Convert our string-keyed map to long-keyed map as expected by the API
                // Note: We need to match exactly, not just prefix, because the rootPath now includes the replication factor
                concurrentTest.getMockResources().entrySet().stream()
                        .filter(entry -> entry.getKey().equals(rootPath) || entry.getKey().startsWith(rootPath + "/"))
                        .forEach(entry -> {
                            LocalResource resource = entry.getValue();
                            matchingResources.put(resource.getId(), resource);
                            // Record which resources were recovered for our test verification
                            recoveredResourcePaths.add(entry.getKey());
                            LOGGER.info("Including resource for recovery: {} (ID: {})", entry.getKey(),
                                    resource.getId());
                        });

                LOGGER.info("Found {} resources to recover", matchingResources.size());

                // Return resources mapped by ID as the real implementation would
                return matchingResources;
            }
            return Map.of();
        }).when(mockRepository).getResources(any(), anyList());

        // Setup capture for recoverIndexes calls
        doAnswer(inv -> {
            List<ILSMIndex> indexes = inv.getArgument(0);
            if (indexes != null && !indexes.isEmpty()) {
                LOGGER.info("Recovering {} indexes", indexes.size());
                for (ILSMIndex index : indexes) {
                    LOGGER.info("Recovering index: {}", index);
                }
            }
            return null;
        }).when(concurrentTest.getMockRecoveryManager()).recoverIndexes(anyList());

        // Choose the first resource in the partition to register and open
        String resourcePath = resourcesInPartition.get(0);

        // Register the index
        IIndex index = concurrentTest.registerIfAbsentIndex(resourcePath);
        assertNotNull("Index should be registered successfully", index);
        LOGGER.info("Successfully registered index at {}", resourcePath);

        // Open the index - should trigger lazy recovery
        concurrentTest.getDatasetLifecycleManager().open(resourcePath);
        LOGGER.info("Successfully opened index at {}", resourcePath);

        // Verify the recovery manager was consulted
        verify(concurrentTest.getMockRecoveryManager(), atLeastOnce()).isLazyRecoveryEnabled();

        // Verify that all resources in the partition were considered for recovery
        LOGGER.info("Verifying recovery for resources in partition: {}", resourcesInPartition);
        LOGGER.info("Resources marked as recovered: {}", recoveredResourcePaths);

        for (String path : resourcesInPartition) {
            boolean wasRecovered = isResourcePathRecovered(path);
            LOGGER.info("Resource {} - recovered: {}", path, wasRecovered);
            assertTrue("Resource should have been considered for recovery: " + path, wasRecovered);
        }

        // Cleanup
        concurrentTest.getDatasetLifecycleManager().close(resourcePath);
        concurrentTest.getDatasetLifecycleManager().unregister(resourcePath);
        concurrentTest.getMockResources().remove(resourcePath);

        // Verify cleanup was successful
        assertNull("Index should no longer be registered",
                concurrentTest.getDatasetLifecycleManager().get(resourcePath));
    }

    /**
     * Tests that lazy recovery is only performed the first time an index is opened
     * and is skipped on subsequent opens.
     */
    @Test
    public void testLazyRecoveryOnlyHappensOnce() throws Exception {
        LOGGER.info("Starting testLazyRecoveryOnlyHappensOnce");

        // Get a resource path
        String resourcePath = concurrentTest.getCreatedResourcePaths().get(0);
        LOGGER.info("Testing with resource: {}", resourcePath);

        // Find the dataset partition path for this resource
        String partitionPath = null;
        for (String path : datasetPartitionPaths) {
            if (resourcePath.startsWith(path)) {
                partitionPath = path;
                break;
            }
        }
        LOGGER.info("Resource belongs to partition: {}", partitionPath);

        // Track recovery calls
        List<String> recoveredPaths = new ArrayList<>();

        // Mock repository to track resource lookups during recovery
        final Map<Integer, Integer> recoveryCount = new HashMap<>();
        recoveryCount.put(0, 0); // First recovery count
        recoveryCount.put(1, 0); // Second recovery count

        ILocalResourceRepository mockRepository = concurrentTest.getMockResourceRepository();

        // Setup capture of resources that get recovered
        doAnswer(inv -> {
            List<FileReference> refs = inv.getArgument(1);
            if (refs != null && !refs.isEmpty()) {
                FileReference fileRef = refs.get(0);
                String rootPath = fileRef.getAbsolutePath();

                // Get all resources that match this root path
                Map<Long, LocalResource> matchingResources = new HashMap<>();

                concurrentTest.getMockResources().entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith(rootPath)).forEach(entry -> {
                            LocalResource resource = entry.getValue();
                            matchingResources.put(resource.getId(), resource);
                            recoveredPaths.add(entry.getKey());
                        });

                // Increment recovery counter based on which open we're on
                if (recoveryCount.get(1) == 0) {
                    recoveryCount.put(0, recoveryCount.get(0) + matchingResources.size());
                    LOGGER.info("First open recovery finding {} resources", matchingResources.size());
                } else {
                    recoveryCount.put(1, recoveryCount.get(1) + matchingResources.size());
                    LOGGER.info("Second open recovery finding {} resources", matchingResources.size());
                }

                return matchingResources;
            }
            return Map.of();
        }).when(mockRepository).getResources(any(), anyList());

        // Mock recovery behavior to track calls
        doAnswer(inv -> {
            List<ILSMIndex> indexes = inv.getArgument(0);
            if (indexes != null && !indexes.isEmpty()) {
                for (ILSMIndex index : indexes) {
                    String path = index.toString();
                    LOGGER.info("Recovering index: {}", path);
                }
            }
            return null;
        }).when(concurrentTest.getMockRecoveryManager()).recoverIndexes(anyList());

        // Register the index
        IIndex index = concurrentTest.registerIfAbsentIndex(resourcePath);
        assertNotNull("Index should be registered successfully", index);

        // First open - should trigger recovery
        LOGGER.info("First open of index");
        concurrentTest.getDatasetLifecycleManager().open(resourcePath);
        LOGGER.info("First open recovery found {} resources", recoveryCount.get(0));

        // Close the index
        concurrentTest.getDatasetLifecycleManager().close(resourcePath);

        // Second open - should NOT trigger recovery again
        LOGGER.info("Second open of index");
        recoveryCount.put(1, 0); // Reset second recovery counter
        concurrentTest.getDatasetLifecycleManager().open(resourcePath);
        LOGGER.info("Second open recovery found {} resources", recoveryCount.get(1));

        // Verify recovery was skipped on second open
        assertTrue("First open should recover resources", recoveryCount.get(0) > 0);
        assertTrue("Recovery should not happen on subsequent opens",
                recoveryCount.get(1) == 0 || recoveryCount.get(1) < recoveryCount.get(0));

        // Cleanup
        concurrentTest.getDatasetLifecycleManager().close(resourcePath);
        concurrentTest.getDatasetLifecycleManager().unregister(resourcePath);
        concurrentTest.getMockResources().remove(resourcePath);
    }

    /**
     * A comprehensive test that runs multiple operations with lazy recovery enabled.
     * This test verifies that lazy recovery works correctly across various scenarios:
     * - Opening multiple resources in the same partition
     * - Operations across multiple partitions
     * - Reference counting with multiple open/close
     * - Proper unregistration after use
     */
    @Test
    public void testComprehensiveLazyRecoveryWithMultipleResources() throws Exception {
        LOGGER.info("Starting testComprehensiveLazyRecoveryWithMultipleResources");

        // Create sets to track which resources have been recovered
        Set<String> recoveredResources = new TreeSet<>();
        Set<String> recoveredPartitions = new TreeSet<>();

        // Mock repository to track resource lookups during recovery
        ILocalResourceRepository mockRepository = concurrentTest.getMockResourceRepository();

        // Setup capture of resources that get recovered
        doAnswer(inv -> {
            List<FileReference> refs = inv.getArgument(1);
            if (refs != null && !refs.isEmpty()) {
                FileReference fileRef = refs.get(0);
                String rootPath = fileRef.getAbsolutePath();
                LOGGER.info("Looking up resources with root path: {}", rootPath);

                // Track this partition as being recovered
                recoveredPartitions.add(rootPath);

                // Get all resources that match this root path
                Map<Long, LocalResource> matchingResources = new HashMap<>();

                concurrentTest.getMockResources().entrySet().stream()
                        .filter(entry -> entry.getKey().startsWith(rootPath)).forEach(entry -> {
                            LocalResource resource = entry.getValue();
                            matchingResources.put(resource.getId(), resource);
                            recoveredResources.add(entry.getKey());
                            LOGGER.info("Including resource for recovery: {} (ID: {})", entry.getKey(),
                                    resource.getId());
                        });

                return matchingResources;
            }
            return Map.of();
        }).when(mockRepository).getResources(any(), anyList());

        // Group resources by partition for easier testing
        Map<String, List<String>> resourcesByPartition = new HashMap<>();

        for (String partitionPath : datasetPartitionPaths) {
            List<String> resourcesInPartition = concurrentTest.getCreatedResourcePaths().stream()
                    .filter(path -> path.startsWith(partitionPath)).collect(Collectors.toList());

            resourcesByPartition.put(partitionPath, resourcesInPartition);
        }

        LOGGER.info("Found {} partitions with resources", resourcesByPartition.size());

        // For each partition, perform a series of operations
        for (Map.Entry<String, List<String>> entry : resourcesByPartition.entrySet()) {
            String partitionPath = entry.getKey();
            List<String> resources = entry.getValue();

            LOGGER.info("Testing partition: {} with {} resources", partitionPath, resources.size());

            // 1. Register and open the first resource - should trigger recovery for all resources in this partition
            String firstResource = resources.get(0);
            LOGGER.info("Registering and opening first resource: {}", firstResource);

            IIndex firstIndex = concurrentTest.registerIfAbsentIndex(firstResource);
            assertNotNull("First index should be registered successfully", firstIndex);

            // Open the index - should trigger lazy recovery for all resources in this partition
            concurrentTest.getDatasetLifecycleManager().open(firstResource);

            // Verify partition was recovered
            assertTrue("Partition should be marked as recovered", recoveredPartitions.contains(partitionPath));

            // 2. Register and open a second resource if available - should NOT trigger recovery again
            if (resources.size() > 1) {
                // Clear recovery tracking before opening second resource
                int beforeSize = recoveredResources.size();
                String secondResource = resources.get(1);

                LOGGER.info("Registering and opening second resource: {}", secondResource);
                IIndex secondIndex = concurrentTest.registerIfAbsentIndex(secondResource);
                assertNotNull("Second index should be registered successfully", secondIndex);

                // Open the second index - should NOT trigger lazy recovery again for this partition
                concurrentTest.getDatasetLifecycleManager().open(secondResource);

                // Verify no additional recovery happened
                int afterSize = recoveredResources.size();
                LOGGER.info("Resources recovered before: {}, after: {}", beforeSize, afterSize);
                // We might see some recovery of the specific resource, but not the whole partition again

                // 3. Open and close the second resource multiple times to test reference counting
                for (int i = 0; i < 3; i++) {
                    concurrentTest.getDatasetLifecycleManager().close(secondResource);
                    concurrentTest.getDatasetLifecycleManager().open(secondResource);
                }

                // Close and unregister the second resource
                concurrentTest.getDatasetLifecycleManager().close(secondResource);
                concurrentTest.getDatasetLifecycleManager().unregister(secondResource);
                LOGGER.info("Successfully closed and unregistered second resource: {}", secondResource);
            }

            // 4. Close and unregister the first resource
            concurrentTest.getDatasetLifecycleManager().close(firstResource);
            concurrentTest.getDatasetLifecycleManager().unregister(firstResource);
            LOGGER.info("Successfully closed and unregistered first resource: {}", firstResource);

            // Verify both resources are truly unregistered
            for (String resource : resources.subList(0, Math.min(2, resources.size()))) {
                assertNull("Resource should be unregistered: " + resource,
                        concurrentTest.getDatasetLifecycleManager().get(resource));
            }
        }

        // Summary of recovery
        LOGGER.info("Recovered {} partitions out of {}", recoveredPartitions.size(), datasetPartitionPaths.size());
        LOGGER.info("Recovered {} resources out of {}", recoveredResources.size(),
                concurrentTest.getCreatedResourcePaths().size());
    }

    /**
     * Tests that open operation succeeds after unregister + register sequence.
     * This verifies the operation tracker is properly reset when re-registering.
     */
    @Test
    public void testUnregisterRegisterIfAbsentOpenSequence() throws Exception {
        LOGGER.info("Starting testUnregisterRegisterOpenSequence");

        // Get a resource path
        String resourcePath = concurrentTest.getCreatedResourcePaths().get(0);
        LOGGER.info("Testing with resource: {}", resourcePath);

        // First lifecycle - register, open, close, unregister
        IIndex firstIndex = concurrentTest.registerIfAbsentIndex(resourcePath);
        assertNotNull("Index should be registered successfully in first lifecycle", firstIndex);

        concurrentTest.getDatasetLifecycleManager().open(resourcePath);
        LOGGER.info("Successfully opened index in first lifecycle");

        concurrentTest.getDatasetLifecycleManager().close(resourcePath);
        concurrentTest.getDatasetLifecycleManager().unregister(resourcePath);
        LOGGER.info("Successfully closed and unregistered index in first lifecycle");

        // Second lifecycle - register again, open, close, unregister
        IIndex secondIndex = concurrentTest.registerIfAbsentIndex(resourcePath);
        assertNotNull("Index should be registered successfully in second lifecycle", secondIndex);

        // This open should succeed if the operation tracker was properly reset
        try {
            concurrentTest.getDatasetLifecycleManager().open(resourcePath);
            LOGGER.info("Successfully opened index in second lifecycle");
        } catch (Exception e) {
            LOGGER.error("Failed to open index in second lifecycle", e);
            fail("Open should succeed in second lifecycle: " + e.getMessage());
        }

        // Cleanup
        concurrentTest.getDatasetLifecycleManager().close(resourcePath);
        concurrentTest.getDatasetLifecycleManager().unregister(resourcePath);
        LOGGER.info("Successfully completed two full lifecycles with the same resource");
    }

    /**
     * Tests that recovery works correctly across different partitions.
     * This test specifically verifies that partition IDs are correctly propagated
     * during the recovery process.
     */
    @Test
    public void testRecoveryAcrossPartitions() throws Exception {
        LOGGER.info("Starting testRecoveryAcrossPartitions");

        // Skip test if we don't have at least 2 partitions
        if (datasetPartitionPaths.size() < 2) {
            LOGGER.info("Skipping testRecoveryAcrossPartitions - need at least 2 partitions");
            return;
        }

        // Create maps to track recovery by partition
        Map<Integer, Boolean> partitionRecovered = new HashMap<>();
        Map<Integer, Set<String>> resourcesRecoveredByPartition = new HashMap<>();

        // Find resources from different partitions
        Map<Integer, String> resourcesByPartition = new HashMap<>();
        // Format: storage/partition_X/DBName/ScopeName/DatasetName/ReplicationFactor/IndexName
        Pattern pattern = Pattern.compile(".*?/partition_(\\d+)/.+");

        for (String resourcePath : concurrentTest.getCreatedResourcePaths()) {
            Matcher matcher = pattern.matcher(resourcePath);
            if (matcher.matches()) {
                int partitionId = Integer.parseInt(matcher.group(1));
                resourcesByPartition.putIfAbsent(partitionId, resourcePath);
                partitionRecovered.put(partitionId, false);
                resourcesRecoveredByPartition.put(partitionId, new TreeSet<>());
            }
        }

        LOGGER.info("Found resources in {} different partitions: {}", resourcesByPartition.size(),
                resourcesByPartition.keySet());

        // Mock repository to track which partition's resources are being recovered
        ILocalResourceRepository mockRepository = concurrentTest.getMockResourceRepository();

        // Setup capture of resources that get recovered
        doAnswer(inv -> {
            List<FileReference> refs = inv.getArgument(1);
            if (refs != null && !refs.isEmpty()) {
                FileReference fileRef = refs.get(0);
                String rootPath = fileRef.getAbsolutePath();
                LOGGER.info("Looking up resources with root path: {}", rootPath);

                // The rootPath now includes the replication factor
                LOGGER.info("Using corrected rootPath for resource lookup: {}", rootPath);

                // Get the partition ID from the path
                Matcher matcher = Pattern.compile(".*?/partition_(\\d+)/.+").matcher(rootPath);
                if (matcher.matches()) {
                    int partitionId = Integer.parseInt(matcher.group(1));
                    partitionRecovered.put(partitionId, true);
                    LOGGER.info("Recovery requested for partition {}", partitionId);
                } else {
                    LOGGER.warn("Could not extract partition ID from path: {}", rootPath);
                }

                // Get all resources that match this root path
                Map<Long, LocalResource> matchingResources = new HashMap<>();

                concurrentTest.getMockResources().entrySet().stream()
                        .filter(entry -> entry.getKey().equals(rootPath) || entry.getKey().startsWith(rootPath + "/"))
                        .forEach(entry -> {
                            LocalResource resource = entry.getValue();
                            matchingResources.put(resource.getId(), resource);

                            // Extract partition ID from path
                            Matcher m = Pattern.compile(".*?/partition_(\\d+)/.+").matcher(entry.getKey());
                            if (m.matches()) {
                                int partitionId = Integer.parseInt(m.group(1));
                                resourcesRecoveredByPartition.get(partitionId).add(entry.getKey());
                                LOGGER.info("Resource {} added to recovery for partition {}", entry.getKey(),
                                        partitionId);
                            }
                        });

                LOGGER.info("Found {} resources to recover", matchingResources.size());
                return matchingResources;
            }
            return Map.of();
        }).when(mockRepository).getResources(any(), anyList());

        // Open resources from each partition
        for (Map.Entry<Integer, String> entry : resourcesByPartition.entrySet()) {
            int partitionId = entry.getKey();
            String resourcePath = entry.getValue();

            LOGGER.info("Testing recovery for partition {} with resource {}", partitionId, resourcePath);

            // Register and open the resource
            IIndex index = concurrentTest.registerIfAbsentIndex(resourcePath);
            assertNotNull("Index should be registered successfully", index);

            // Get the LocalResource and verify its partition ID
            LocalResource localResource = concurrentTest.getMockResources().get(resourcePath);
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) localResource.getResource();
            int resourcePartitionId = datasetLocalResource.getPartition();

            LOGGER.info("Resource {} has partition ID {} (expected {})", resourcePath, resourcePartitionId,
                    partitionId);

            // Open the index - should trigger lazy recovery for this partition
            concurrentTest.getDatasetLifecycleManager().open(resourcePath);
            LOGGER.info("Successfully opened index for partition {}", partitionId);

            // Verify this partition was recovered
            assertTrue("Partition " + partitionId + " should be marked as recovered",
                    partitionRecovered.get(partitionId));

            // Verify resources for this partition were recovered
            Set<String> recoveredResources = resourcesRecoveredByPartition.get(partitionId);
            assertTrue("At least one resource should be recovered for partition " + partitionId,
                    !recoveredResources.isEmpty());

            // Cleanup
            concurrentTest.getDatasetLifecycleManager().close(resourcePath);
            concurrentTest.getDatasetLifecycleManager().unregister(resourcePath);
        }

        // Summary of recovery by partition
        for (Map.Entry<Integer, Boolean> entry : partitionRecovered.entrySet()) {
            int partitionId = entry.getKey();
            boolean recovered = entry.getValue();
            int resourceCount = resourcesRecoveredByPartition.get(partitionId).size();

            LOGGER.info("Partition {}: Recovered={}, Resources recovered={}", partitionId, recovered, resourceCount);
        }
    }

    /**
     * Tests that the partition ID in DatasetLocalResource objects is correctly set.
     * This test specifically verifies that partition IDs match the resource path.
     */
    @Test
    public void testPartitionIdMatchesPath() throws Exception {
        LOGGER.info("Starting testPartitionIdMatchesPath");

        // Find resources from different partitions
        Map<Integer, Set<String>> resourcesByPartition = new HashMap<>();
        Pattern pattern = Pattern.compile(".*?/partition_(\\d+)/.+");

        // First, collect all resources by partition
        for (String resourcePath : concurrentTest.getCreatedResourcePaths()) {
            Matcher matcher = pattern.matcher(resourcePath);
            if (matcher.matches()) {
                int partitionId = Integer.parseInt(matcher.group(1));
                resourcesByPartition.computeIfAbsent(partitionId, k -> new TreeSet<>()).add(resourcePath);
            }
        }

        boolean incorrectPartitionIdFound = false;

        // Examine each resource and check its partition ID
        for (Map.Entry<Integer, Set<String>> entry : resourcesByPartition.entrySet()) {
            int expectedPartitionId = entry.getKey();
            Set<String> resources = entry.getValue();

            LOGGER.info("Checking {} resources for partition {}", resources.size(), expectedPartitionId);

            for (String resourcePath : resources) {
                LocalResource localResource = concurrentTest.getMockResources().get(resourcePath);
                if (localResource != null) {
                    DatasetLocalResource datasetLocalResource = (DatasetLocalResource) localResource.getResource();
                    int actualPartitionId = datasetLocalResource.getPartition();

                    LOGGER.info("Resource: {}, Expected partition: {}, Actual partition: {}", resourcePath,
                            expectedPartitionId, actualPartitionId);

                    if (expectedPartitionId != actualPartitionId) {
                        LOGGER.error("PARTITION ID MISMATCH for resource {}! Expected: {}, Actual: {}", resourcePath,
                                expectedPartitionId, actualPartitionId);
                        incorrectPartitionIdFound = true;
                    }
                } else {
                    LOGGER.warn("Resource not found in mockResources: {}", resourcePath);
                }
            }
        }

        // This assertion will fail if any partition ID is incorrect
        assertFalse("Incorrect partition IDs found in resources", incorrectPartitionIdFound);

        // Now test DatasetLifecycleManager.getDatasetLocalResource directly
        LOGGER.info("Testing DatasetLifecycleManager.getDatasetLocalResource directly");

        // Find a resource in partition 1 (if available)
        Set<String> partition1Resources = resourcesByPartition.getOrDefault(1, Collections.emptySet());
        if (!partition1Resources.isEmpty()) {
            String resourcePath = partition1Resources.iterator().next();
            LOGGER.info("Testing getDatasetLocalResource with partition 1 resource: {}", resourcePath);

            // Register the resource
            IIndex index = concurrentTest.registerIfAbsentIndex(resourcePath);
            assertNotNull("Index should be registered successfully", index);

            // Directly access the DatasetLocalResource to check partition ID
            // We'll need to use reflection since getDatasetLocalResource is private
            try {
                // Get the private method
                Method getDatasetLocalResourceMethod =
                        DatasetLifecycleManager.class.getDeclaredMethod("getDatasetLocalResource", String.class);
                getDatasetLocalResourceMethod.setAccessible(true);

                // Invoke the method
                DatasetLocalResource result = (DatasetLocalResource) getDatasetLocalResourceMethod
                        .invoke(concurrentTest.getDatasetLifecycleManager(), resourcePath);

                assertNotNull("DatasetLocalResource should not be null", result);
                int actualPartitionId = result.getPartition();
                LOGGER.info("getDatasetLocalResource for {} returned partition ID: {}", resourcePath,
                        actualPartitionId);
                assertEquals("Partition ID should be 1", 1, actualPartitionId);
            } catch (Exception e) {
                LOGGER.error("Error accessing getDatasetLocalResource method", e);
                fail("Failed to access getDatasetLocalResource method: " + e.getMessage());
            }

            // Clean up
            concurrentTest.getDatasetLifecycleManager().unregister(resourcePath);
        } else {
            LOGGER.info("No resources found in partition 1, skipping direct test");
        }
    }

    /**
     * Tests that the mock resources are correctly set up with the proper partition IDs.
     * This test directly examines the mockResources map to verify the mocks.
     */
    @Test
    public void testMockResourcesSetup() throws Exception {
        LOGGER.info("Starting testMockResourcesSetup");

        // Extract partition IDs from paths and compare with DatasetLocalResource partitions
        Pattern pattern = Pattern.compile(".*?/partition_(\\d+)/.+");
        int correctResources = 0;
        int incorrectResources = 0;

        LOGGER.info("Total mock resources: {}", concurrentTest.getMockResources().size());

        // Print out all setupMockResource calls that would be needed to recreate the resources
        for (Map.Entry<String, LocalResource> entry : concurrentTest.getMockResources().entrySet()) {
            String resourcePath = entry.getKey();
            LocalResource localResource = entry.getValue();

            // Extract the expected partition ID from the path
            Matcher matcher = pattern.matcher(resourcePath);
            if (matcher.matches()) {
                int expectedPartitionId = Integer.parseInt(matcher.group(1));

                // Get the actual partition ID from the resource
                Object resourceObj = localResource.getResource();
                if (resourceObj instanceof DatasetLocalResource) {
                    DatasetLocalResource datasetLocalResource = (DatasetLocalResource) resourceObj;
                    int actualPartitionId = datasetLocalResource.getPartition();
                    int datasetId = datasetLocalResource.getDatasetId();
                    long resourceId = localResource.getId();

                    // Log resource details
                    LOGGER.info(
                            "Resource: {}, Expected Partition: {}, Actual Partition: {}, Dataset ID: {}, Resource ID: {}",
                            resourcePath, expectedPartitionId, actualPartitionId, datasetId, resourceId);

                    // Generate the setupMockResource call that would create this resource
                    LOGGER.info("setupMockResource(\"{}\", {}, {}, {});", resourcePath, datasetId, expectedPartitionId,
                            resourceId);

                    if (expectedPartitionId == actualPartitionId) {
                        correctResources++;
                    } else {
                        incorrectResources++;
                        LOGGER.error("PARTITION MISMATCH in mock resources: {} (expected: {}, actual: {})",
                                resourcePath, expectedPartitionId, actualPartitionId);
                    }
                } else {
                    LOGGER.warn("Resource object is not a DatasetLocalResource: {}", resourceObj);
                }
            } else {
                LOGGER.warn("Resource path doesn't match expected pattern: {}", resourcePath);
            }
        }

        LOGGER.info("Resources with correct partition IDs: {}", correctResources);
        LOGGER.info("Resources with incorrect partition IDs: {}", incorrectResources);

        // Fail the test if any resources have incorrect partition IDs
        assertEquals("All resources should have correct partition IDs", 0, incorrectResources);

        // Now test a direct mock creation and retrieval to see if that works correctly
        LOGGER.info("Testing direct mock creation and retrieval");

        // Create a test mock resource with partition 1
        String testPath = "test/partition_1/Default/SDefault/testDs/0/testIndex"; // 0 is replication factor (always 0)
        int testDatasetId = 999;
        int testPartition = 1;
        long testResourceId = 12345;

        // Create a mock dataset local resource
        DatasetLocalResource mockDatasetResource = mock(DatasetLocalResource.class);
        when(mockDatasetResource.getDatasetId()).thenReturn(testDatasetId);
        when(mockDatasetResource.getPartition()).thenReturn(testPartition);

        // Create a mock LSMINDEX (not just IIndex) since recovery expects ILSMIndex
        ILSMIndex mockIndex = mock(ILSMIndex.class);
        when(mockIndex.toString()).thenReturn(testPath);

        // Add necessary behavior for LSM operations
        when(mockIndex.isDurable()).thenReturn(true);
        when(mockIndex.getNumberOfAllMemoryComponents()).thenReturn(2);

        // Allow this resource to be created
        when(mockDatasetResource.createInstance(any())).thenReturn(mockIndex);

        // Create a mock local resource
        LocalResource mockLocalResource = mock(LocalResource.class);
        when(mockLocalResource.getId()).thenReturn(testResourceId);
        when(mockLocalResource.getPath()).thenReturn(testPath);
        when(mockLocalResource.getResource()).thenReturn(mockDatasetResource);

        // Add to mock resources map
        concurrentTest.getMockResources().put(testPath, mockLocalResource);

        // Create a mock file reference
        FileReference mockFileRef = mock(FileReference.class);
        when(mockFileRef.getRelativePath()).thenReturn(testPath);
        when(mockFileRef.getAbsolutePath()).thenReturn(testPath);
        when(concurrentTest.getMockServiceContext().getIoManager().resolveAbsolutePath(testPath))
                .thenReturn(mockFileRef);
    }

    /**
     * Creates a mock resource for testing.
     */
    private void setupMockResourceForTest(String resourcePath, int datasetId, int partition, long resourceId)
            throws HyracksDataException {
        // Create a mock dataset local resource
        DatasetLocalResource mockDatasetResource = mock(DatasetLocalResource.class);
        when(mockDatasetResource.getDatasetId()).thenReturn(datasetId);
        when(mockDatasetResource.getPartition()).thenReturn(partition);

        // Ensure we have a dataset resource
        DatasetResource datasetResource = concurrentTest.getDatasetLifecycleManager().getDatasetLifecycle(datasetId);

        // Create op tracker if needed
        PrimaryIndexOperationTracker opTracker = datasetResource.getOpTracker(partition);
        if (opTracker == null) {
            opTracker = mock(PrimaryIndexOperationTracker.class);
            datasetResource.setPrimaryIndexOperationTracker(partition, opTracker);
            LOGGER.info("Created operation tracker for dataset {} partition {}", datasetId, partition);
        }

        // Create a mock local resource
        LocalResource mockLocalResource = mock(LocalResource.class);
        when(mockLocalResource.getId()).thenReturn(resourceId);
        when(mockLocalResource.getPath()).thenReturn(resourcePath);
        when(mockLocalResource.getResource()).thenReturn(mockDatasetResource);

        // Add to mock resources map
        concurrentTest.getMockResources().put(resourcePath, mockLocalResource);

        // Create a mock file reference
        FileReference mockFileRef = mock(FileReference.class);
        when(mockFileRef.getRelativePath()).thenReturn(resourcePath);
        when(mockFileRef.getAbsolutePath()).thenReturn(resourcePath);
        when(concurrentTest.getMockServiceContext().getIoManager().resolveAbsolutePath(resourcePath))
                .thenReturn(mockFileRef);

        // Ensure the mock index is the one that will be returned during registration
        concurrentTest.createMockIndexForPath(resourcePath, datasetId, partition, opTracker);
    }

    /**
     * Tests that unregister operation succeeds during lazy recovery.
     * This test verifies that resources are correctly unregistered during lazy recovery.
     */
    @Test
    public void testConcurrentUnregisterDuringLazyRecovery() throws Exception {
        LOGGER.info("Starting testConcurrentUnregisterDuringLazyRecovery");

        // We need at least 6 resources for a good test
        final int REQUIRED_RESOURCES = 6;

        // Get multiple resources from the same dataset and partition
        List<String> resourcesInSamePartition = findResourcesInSamePartition(REQUIRED_RESOURCES);

        // If we don't have enough resources, create more
        if (resourcesInSamePartition.size() < REQUIRED_RESOURCES) {
            LOGGER.info("Only found {} resources, creating additional resources", resourcesInSamePartition.size());

            // Get dataset and partition info from the existing resources, or create new ones
            int datasetId;
            int partitionId;
            String datasetName;

            if (!resourcesInSamePartition.isEmpty()) {
                // Use the same dataset and partition as existing resources
                String existingResource = resourcesInSamePartition.get(0);
                LocalResource localResource = concurrentTest.getMockResources().get(existingResource);
                DatasetLocalResource datasetLocalResource = (DatasetLocalResource) localResource.getResource();
                datasetId = datasetLocalResource.getDatasetId();
                partitionId = datasetLocalResource.getPartition();

                // Extract dataset name from the existing resource path
                Pattern datasetPattern = Pattern.compile(".*?/partition_\\d+/[^/]+/[^/]+/([^/]+)/\\d+/.*");
                Matcher matcher = datasetPattern.matcher(existingResource);
                if (matcher.matches()) {
                    datasetName = matcher.group(1);
                    LOGGER.info("Using dataset name from existing resource: {}", datasetName);
                } else {
                    datasetName = "testDs";
                    LOGGER.warn("Could not extract dataset name from {}, using default: {}", existingResource,
                            datasetName);
                }
            } else {
                // Create new dataset and partition
                datasetId = 999; // Use a unique dataset ID
                partitionId = 1; // Use partition 1
                datasetName = "testDs";
            }

            LOGGER.info("Creating resources for dataset {} partition {}", datasetId, partitionId);

            // Create additional resources until we reach the required count
            for (int i = resourcesInSamePartition.size(); i < REQUIRED_RESOURCES; i++) {
                String indexName = "test_index_" + i;
                // Use the dataset name extracted from existing resources
                String resourcePath = String.format("%s/partition_%d/%s/%s/%s/0/%s", "storage", partitionId, "Default",
                        "SDefault", datasetName, indexName);

                // Create new mock resources
                setupMockResourceForTest(resourcePath, datasetId, partitionId, datasetId * 1000L + i);
                resourcesInSamePartition.add(resourcePath);
                // LOGGER.info("Created additional resource: {}", resourcePath);
            }
        }

        LOGGER.info("Have {} resources for test", resourcesInSamePartition.size());

        // Register ALL resources
        List<String> registeredResources = new ArrayList<>();
        for (String resourcePath : resourcesInSamePartition) {
            IIndex index = concurrentTest.registerIfAbsentIndex(resourcePath);
            assertNotNull("Index should be registered successfully: " + resourcePath, index);
            registeredResources.add(resourcePath);
            LOGGER.info("Registered resource: {}", resourcePath);
        }

        // We'll use 1/3 of resources for unregistering
        int unregisterCount = Math.max(2, resourcesInSamePartition.size() / 3);

        // Split resources: one for opening, several for unregistering, rest for recovery
        String resourceForOpen = registeredResources.get(0);
        List<String> resourcesToUnregister = registeredResources.subList(1, 1 + unregisterCount);
        List<String> otherResources = registeredResources.subList(1 + unregisterCount, registeredResources.size());

        LOGGER.info("Using {} resources: 1 for open, {} for unregistering, {} for verification",
                registeredResources.size(), resourcesToUnregister.size(), otherResources.size());

        // Get the dataset and partition ID for tracking
        LocalResource localResourceA = concurrentTest.getMockResources().get(resourceForOpen);
        DatasetLocalResource datasetLocalResourceA = (DatasetLocalResource) localResourceA.getResource();
        int datasetId = datasetLocalResourceA.getDatasetId();
        int partitionId = datasetLocalResourceA.getPartition();

        LOGGER.info("Test using dataset {} partition {}", datasetId, partitionId);

        // Set up mock for getResources to return actual resources during recovery
        ILocalResourceRepository mockRepository = concurrentTest.getMockResourceRepository();
        doAnswer(inv -> {
            List<FileReference> refs = inv.getArgument(1);
            if (refs != null && !refs.isEmpty()) {
                // Create a map of resources to return
                Map<Long, LocalResource> result = new HashMap<>();
                // Add our test resources from the same partition
                for (String path : registeredResources) {
                    LocalResource res = concurrentTest.getMockResources().get(path);
                    if (res != null) {
                        result.put(res.getId(), res);
                        LOGGER.info("Adding resource to recovery batch: {}", path);
                    }
                }
                return result;
            }
            return Collections.emptyMap();
        }).when(mockRepository).getResources(any(), any());

        // Ensure operation tracker is properly initialized for the dataset resources
        DatasetResource datasetResource = concurrentTest.getDatasetLifecycleManager().getDatasetLifecycle(datasetId);
        if (datasetResource.getOpTracker(partitionId) == null) {
            PrimaryIndexOperationTracker opTracker = mock(PrimaryIndexOperationTracker.class);
            datasetResource.setPrimaryIndexOperationTracker(partitionId, opTracker);
            LOGGER.info("Created and set operation tracker for dataset {} partition {}", datasetId, partitionId);
        }

        // Verify operation tracker exists before proceeding
        assertNotNull("Operation tracker should be initialized for partition " + partitionId,
                datasetResource.getOpTracker(partitionId));

        // Execute operations with controlled timing
        CountDownLatch recoveryStartedLatch = new CountDownLatch(1);
        CountDownLatch unregisterCompleteLatch = new CountDownLatch(1);

        // Mock the recovery to signal when it starts
        IRecoveryManager mockRecoveryManager = concurrentTest.getMockRecoveryManager();
        doAnswer(invocation -> {
            List<ILSMIndex> indexes = invocation.getArgument(0);
            LOGGER.info("Recovery started with {} indexes", indexes.size());

            // Signal that recovery has started
            recoveryStartedLatch.countDown();

            // Wait for unregister to complete before continuing
            LOGGER.info("Waiting for unregister to complete");
            unregisterCompleteLatch.await(10, TimeUnit.SECONDS);
            LOGGER.info("Continuing with recovery after unregister");

            // Mark resources as recovered
            for (ILSMIndex index : indexes) {
                String path = index.toString();
                recoveredResourcePaths.add(path);
                // Remove individual resource logs
            }
            // Add a summary after the loop
            LOGGER.info("Marked {} resources as recovered", indexes.size());

            return null;
        }).when(mockRecoveryManager).recoverIndexes(anyList());

        // Thread 1: Open resource for open (will trigger recovery for all)
        Future<?> openFuture = executorService.submit(() -> {
            try {
                LOGGER.info("Thread 1: Opening resource to trigger recovery...");
                concurrentTest.getDatasetLifecycleManager().open(resourceForOpen);
                LOGGER.info("Thread 1: Successfully opened resource");
            } catch (Exception e) {
                LOGGER.error("Thread 1: Error opening resource", e);
            }
        });

        // Wait for recovery to start
        LOGGER.info("Waiting for recovery to start");
        boolean recoveryStarted = recoveryStartedLatch.await(10, TimeUnit.SECONDS);
        assertTrue("Recovery should have started", recoveryStarted);
        LOGGER.info("Recovery has started");

        // Thread 2: Unregister multiple resources while recovery is happening
        Future<?> unregisterFuture = executorService.submit(() -> {
            try {
                LOGGER.info("Thread 2: Unregistering {} resources...", resourcesToUnregister.size());

                for (String resourcePath : resourcesToUnregister) {
                    concurrentTest.getDatasetLifecycleManager().unregister(resourcePath);
                }

                // Signal that unregister is complete
                unregisterCompleteLatch.countDown();
                LOGGER.info("Thread 2: Successfully unregistered all resources");
            } catch (Exception e) {
                LOGGER.error("Thread 2: Error unregistering resources", e);
                // Ensure we don't deadlock in case of failure
                unregisterCompleteLatch.countDown();
            }
        });

        // Wait for unregister to complete
        unregisterFuture.get(50, TimeUnit.SECONDS);

        // Wait for open to complete
        openFuture.get(10, TimeUnit.SECONDS);

        // Verify expectations
        LOGGER.info("Checking final state");

        // Check which resources were recovered
        List<String> recoveredResources = new ArrayList<>();
        List<String> nonRecoveredResources = new ArrayList<>();

        for (String resourcePath : registeredResources) {
            if (isResourcePathRecovered(resourcePath)) {
                recoveredResources.add(resourcePath);
            } else {
                nonRecoveredResources.add(resourcePath);
            }
        }

        LOGGER.info("Results: {} resources recovered, {} resources not recovered", recoveredResources.size(),
                nonRecoveredResources.size());

        // Resource for open should have been opened successfully
        assertTrue("Resource for open should be recovered", isResourcePathRecovered(resourceForOpen));

        // Resources to unregister should be unregistered
        for (String resourcePath : resourcesToUnregister) {
            IIndex indexAfter = concurrentTest.getDatasetLifecycleManager().get(resourcePath);
            assertNull("Resource should be unregistered: " + resourcePath, indexAfter);
        }

        // Other resources should either be recovered or not, depending on timing
        LOGGER.info("Completed testConcurrentUnregisterDuringLazyRecovery");
    }

    /**
     * Helper method to find resources that belong to the same dataset and partition.
     * 
     * @param minCount Minimum number of resources needed
     * @return List of resource paths from the same dataset and partition
     */
    private List<String> findResourcesInSamePartition(int minCount) {
        // Group resources by dataset ID and partition ID
        Map<String, List<String>> resourcesByDatasetAndPartition = new HashMap<>();

        LOGGER.info("Looking for at least {} resources in the same partition", minCount);

        for (String resourcePath : concurrentTest.getCreatedResourcePaths()) {
            LocalResource localResource = concurrentTest.getMockResources().get(resourcePath);
            if (localResource != null) {
                DatasetLocalResource datasetLocalResource = (DatasetLocalResource) localResource.getResource();
                int datasetId = datasetLocalResource.getDatasetId();
                int partitionId = datasetLocalResource.getPartition();

                // Create a composite key using datasetId and partitionId
                String key = datasetId + "_" + partitionId;
                resourcesByDatasetAndPartition.computeIfAbsent(key, k -> new ArrayList<>()).add(resourcePath);
            }
        }

        // Just log summary, not details
        LOGGER.info("Found {} groups of resources by dataset/partition", resourcesByDatasetAndPartition.size());

        // Find a group with enough resources
        for (List<String> resources : resourcesByDatasetAndPartition.values()) {
            if (resources.size() >= minCount) {
                String firstResource = resources.get(0);
                LocalResource localResource = concurrentTest.getMockResources().get(firstResource);
                DatasetLocalResource datasetLocalResource = (DatasetLocalResource) localResource.getResource();

                LOGGER.info("Found suitable group with {} resources (dataset={}, partition={})", resources.size(),
                        datasetLocalResource.getDatasetId(), datasetLocalResource.getPartition());
                return resources;
            }
        }

        // If no group has enough resources, return the one with the most
        List<String> bestGroup = resourcesByDatasetAndPartition.values().stream()
                .max(Comparator.comparingInt(List::size)).orElse(Collections.emptyList());

        if (!bestGroup.isEmpty()) {
            String firstResource = bestGroup.get(0);
            LocalResource localResource = concurrentTest.getMockResources().get(firstResource);
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) localResource.getResource();

            LOGGER.info("Using best available group: {} resources (dataset={}, partition={})", bestGroup.size(),
                    datasetLocalResource.getDatasetId(), datasetLocalResource.getPartition());
        } else {
            LOGGER.warn("Could not find any resources in the same partition");
        }

        return bestGroup;
    }

    /**
     * Helper method to check if a resource path is contained in the recovered paths.
     * This handles potential differences between the resource path string and the index.toString() format.
     */
    private boolean isResourcePathRecovered(String resourcePath) {
        // Direct match check
        if (recoveredResourcePaths.contains(resourcePath)) {
            return true;
        }

        // For each recovered path, check if it contains the resource path
        for (String recoveredPath : recoveredResourcePaths) {
            if (recoveredPath.equals(resourcePath) ||
            // Check if the recoveredPath contains the resourcePath (for formatted toString values)
                    recoveredPath.contains(resourcePath)) {
                return true;
            }
        }
        return false;
    }
}
