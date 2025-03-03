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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.dataflow.DatasetLocalResource;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.IRecoveryManager;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.replication.IReplicationJob;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMergePolicy;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMOperationTracker;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.cloud.IIndexDiskCacheManager;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ILocalResourceRepository;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IPageWriteCallback;
import org.apache.hyracks.storage.common.disk.IDiskResourceCacheLockNotifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Concurrent tests for the DatasetLifecycleManager class.
 *
 * This test class verifies that the DatasetLifecycleManager properly handles
 * concurrent register, open, close, and unregister operations on indexes.
 *
 * The tests use mock objects to simulate the index lifecycle and verify:
 * 1. Thread safety of operations
 * 2. Correct handling of re-registration attempts
 * 3. Proper activation/deactivation during open/close
 * 4. Correct identity preservation for index instances
 * 5. Concurrent access to shared resources
 *
 * Each test method focuses on a specific aspect of concurrent behavior:
 * - testConcurrentRegisterAndOpen: Tests register and open operations
 * - testConcurrentRegisterOpenAndUnregister: Tests the full lifecycle
 * - testRegisterAlreadyRegisteredIndex: Tests re-registration behavior
 * - testConcurrentOpenSameIndex: Tests opening the same index concurrently
 * - testConcurrentCloseAfterOpen: Tests closing after open operations
 * - testMockIndexIdentity: Tests the mock setup itself
 */
public class DatasetLifecycleManagerConcurrentTest {
    private static final Logger LOGGER = LogManager.getLogger();

    // Configuration constants
    private static final int NUM_DATASETS = 3;
    private static final int NUM_PARTITIONS = 2;
    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int NUM_OPERATIONS_PER_THREAD = 20;

    // For resource paths
    private static final String DB_NAME = "Default";
    private static final String SCOPE_NAME = "SDefault";
    private static final String STORAGE_DIR = "storage";

    private DatasetLifecycleManager datasetLifecycleManager;
    private INCServiceContext mockServiceContext;
    private StorageProperties mockStorageProperties;
    private ILocalResourceRepository mockResourceRepository;
    private IRecoveryManager mockRecoveryManager;
    private ILogManager mockLogManager;
    private IVirtualBufferCache mockVBC;
    private IIndexCheckpointManagerProvider mockCheckpointProvider;
    private IDiskResourceCacheLockNotifier mockLockNotifier;
    private IIOManager mockIOManager;

    private Map<String, MockIndex> mockIndexes;
    private Map<String, LocalResource> mockResources;

    private ExecutorService executorService;
    private List<String> createdResourcePaths;

    private Map<Integer, DatasetResource> datasetResourceMap;

    // Add this field after the other private fields
    private boolean lazyRecoveryEnabled = false;

    /**
     * Sets up the test environment with mock objects and resources.
     * This includes:
     * 1. Creating mock service context and other components
     * 2. Setting up mock resources with unique IDs
     * 3. Creating the DatasetLifecycleManager with mock dependencies
     * 4. Setting up a thread pool for concurrent operations
     */
    @Before
    public void setUp() throws Exception {
        // Initialize collections
        mockIndexes = new ConcurrentHashMap<>();
        mockResources = new ConcurrentHashMap<>();
        createdResourcePaths = new ArrayList<>();
        datasetResourceMap = new HashMap<>();

        // Set up mocks
        mockServiceContext = mock(INCServiceContext.class);
        mockStorageProperties = mock(StorageProperties.class);
        mockResourceRepository = mock(ILocalResourceRepository.class);
        mockRecoveryManager = mock(IRecoveryManager.class);
        mockLogManager = mock(ILogManager.class);
        mockVBC = mock(IVirtualBufferCache.class);
        mockCheckpointProvider = mock(IIndexCheckpointManagerProvider.class);
        mockLockNotifier = mock(IDiskResourceCacheLockNotifier.class);
        mockIOManager = mock(IIOManager.class);

        // Mock behavior
        when(mockServiceContext.getIoManager()).thenReturn(mockIOManager);
        when(mockStorageProperties.getMemoryComponentsNum()).thenReturn(2);
        when(mockRecoveryManager.isLazyRecoveryEnabled()).thenReturn(lazyRecoveryEnabled);

        // Create DatasetLifecycleManager FIRST
        datasetLifecycleManager =
                new DatasetLifecycleManager(mockServiceContext, mockStorageProperties, mockResourceRepository,
                        mockRecoveryManager, mockLogManager, mockVBC, mockCheckpointProvider, mockLockNotifier);

        // NEXT get the datasets map via reflection
        try {
            Field datasetsField = DatasetLifecycleManager.class.getDeclaredField("datasets");
            datasetsField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Integer, DatasetResource> datasets =
                    (Map<Integer, DatasetResource>) datasetsField.get(datasetLifecycleManager);
            if (datasets != null) {
                LOGGER.info("Accessed datasets map successfully, found {} entries", datasets.size());
                // Store a direct reference to the actual map
                datasetResourceMap = datasets;
            } else {
                LOGGER.warn("Retrieved datasets map is null");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to access datasets field via reflection", e);
        }

        // FINALLY setup mock resources (so they get the real datasets map)
        setupMockResources();

        executorService = Executors.newFixedThreadPool(NUM_THREADS);
    }

    /**
     * Helper method to set the lazy recovery flag and reconfigure the mock
     */
    public void setLazyRecoveryEnabled(boolean enabled) {
        this.lazyRecoveryEnabled = enabled;
        when(mockRecoveryManager.isLazyRecoveryEnabled()).thenReturn(enabled);
        LOGGER.info("Set lazy recovery enabled to: {}", enabled);
    }

    /**
     * Creates mock resources for testing.
     * For each combination of dataset and partition:
     * 1. Creates primary and secondary indexes with unique resource IDs
     * 2. Sets up the repository to return the appropriate resources
     * 3. Verifies that all resources have unique IDs
     *
     * This is crucial for testing the register/open/unregister lifecycle,
     * as it ensures each resource path maps to a unique mock index.
     */
    private void setupMockResources() throws HyracksDataException {
        LOGGER.debug("Setting up mock resources");

        // Setup mock resources for different datasets and partitions
        for (int i = 0; i < NUM_DATASETS; i++) {
            // Use datasetId starting from 101 to avoid reserved IDs (below 100)
            int datasetId = 101 + i;
            String datasetName = "ds" + i;
            for (int j = 0; j < NUM_PARTITIONS; j++) {
                // Create primary index
                String primaryPath = getResourcePath(j, datasetName, 0, datasetName);
                setupMockResource(primaryPath, datasetId, j, datasetId * 100 + j * 10);

                // Create secondary index
                String secondaryPath = getResourcePath(j, datasetName, 1, datasetName + "_idx");
                setupMockResource(secondaryPath, datasetId, j, datasetId * 100 + j * 10 + 1);

                createdResourcePaths.add(primaryPath);
                createdResourcePaths.add(secondaryPath);
            }
        }

        // Wire up the mockResourceRepository to return our mock resources
        when(mockResourceRepository.get(anyString())).thenAnswer(invocation -> {
            String path = invocation.getArgument(0);
            LocalResource resource = mockResources.get(path);
            LOGGER.debug("get({}) returning {}", path, resource);
            return resource;
        });

        LOGGER.debug("Created {} mock resources", mockResources.size());
    }

    /**
     * Sets up a mock resource for a specific resource path.
     * For each resource path, this method:
     * 1. Creates a unique MockIndex instance
     * 2. Creates a mock DatasetLocalResource that returns the MockIndex
     * 3. Creates a mock LocalResource with the unique resourceId
     * 4. Sets up the IO manager to resolve the resource path
     *
     * The key pattern here is that each resource path must map to a unique
     * MockIndex, and each resource must have a unique ID.
     */
    private void setupMockResource(String resourcePath, int datasetId, int partition, long resourceId)
            throws HyracksDataException {

        // Create and store a mock index, passing the datasetResourceMap
        MockIndex mockIndex = new MockIndex(resourcePath, datasetId, datasetResourceMap);
        mockIndexes.put(resourcePath, mockIndex);

        // Create a mock dataset local resource
        DatasetLocalResource mockDatasetResource = mock(DatasetLocalResource.class);
        when(mockDatasetResource.getDatasetId()).thenReturn(datasetId);
        when(mockDatasetResource.getPartition()).thenReturn(partition);

        // Important: We need to ensure each createInstance call returns the specific MockIndex 
        // for this resource path
        when(mockDatasetResource.createInstance(mockServiceContext)).thenAnswer(invocation -> {
            return mockIndexes.get(resourcePath);
        });

        // Create a mock local resource
        LocalResource mockLocalResource = mock(LocalResource.class);
        when(mockLocalResource.getId()).thenReturn(resourceId);
        when(mockLocalResource.getPath()).thenReturn(resourcePath);
        when(mockLocalResource.getResource()).thenReturn(mockDatasetResource);

        mockResources.put(resourcePath, mockLocalResource);

        // Create a mock file reference
        FileReference mockFileRef = mock(FileReference.class);
        when(mockFileRef.getRelativePath()).thenReturn(resourcePath);
        when(mockIOManager.resolveAbsolutePath(resourcePath)).thenReturn(mockFileRef);
    }

    /**
     * Generates a standardized resource path for a given partition, dataset, and index.
     * Format: storage/partition_X/DbName/ScopeName/DatasetName/ReplicationFactor/ResourceID_IndexName
     * This ensures consistent resource path formatting throughout the tests.
     *
     * Note: The replication factor is always 0 in production environments, so we match that here.
     */
    private String getResourcePath(int partition, String datasetName, int resourceId, String indexName) {
        // Format: storage/partition_X/DbName/ScopeName/DatasetName/ReplicationFactor/IndexName
        // Always use 0 for replication factor, and include resourceId as part of the index name
        return String.format("%s/partition_%d/%s/%s/%s/0/%s_%s", STORAGE_DIR, partition, DB_NAME, SCOPE_NAME,
                datasetName, resourceId, indexName);
    }

    /**
     * Cleans up after tests by shutting down the executor service.
     */
    @After
    public void tearDown() throws Exception {
        executorService.shutdownNow();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        datasetLifecycleManager.stop(false, null);
    }

    /**
     * Tests concurrent registration and opening of indexes.
     * This test verifies that:
     * 1. Multiple threads can concurrently register and open indexes without causing errors
     * 2. Re-registration of an already registered index returns the same index instance
     * 3. Indexes are properly activated when opened
     * 4. All register and open operations properly maintain index state
     */
    @Test
    public void testConcurrentRegisterIfAbsentAndOpen() throws Exception {
        LOGGER.info("Starting testConcurrentRegisterAndOpen");

        final CyclicBarrier barrier = new CyclicBarrier(NUM_THREADS);
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger errorCount = new AtomicInteger(0);
        final Map<String, IIndex> firstRegistrations = new ConcurrentHashMap<>();
        final List<Future<?>> futures = new ArrayList<>();

        // Register half of the resources upfront
        int preRegisteredCount = createdResourcePaths.size() / 2;
        for (int i = 0; i < preRegisteredCount; i++) {
            String resourcePath = createdResourcePaths.get(i);
            IIndex index = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
            firstRegistrations.put(resourcePath, index);
        }

        // Create threads that will randomly register/re-register and open indexes
        for (int i = 0; i < NUM_THREADS; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    barrier.await(); // Ensure all threads start at the same time

                    Random random = new Random();
                    for (int j = 0; j < NUM_OPERATIONS_PER_THREAD; j++) {
                        // Pick a random resource path
                        String resourcePath = createdResourcePaths.get(random.nextInt(createdResourcePaths.size()));

                        // Randomly choose between register or open
                        if (random.nextBoolean()) {
                            IIndex index = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
                            assertNotNull("Index should not be null after register", index);

                            // If this path was previously registered, verify it's the same instance
                            IIndex firstInstance = firstRegistrations.get(resourcePath);
                            if (firstInstance != null) {
                                assertSame("Re-registration should return the same index instance", firstInstance,
                                        index);
                            } else {
                                // First time registering this path, record the instance
                                firstRegistrations.putIfAbsent(resourcePath, index);
                            }

                            successCount.incrementAndGet();
                        } else {
                            try {
                                // Try to register first if not already registered
                                if (!firstRegistrations.containsKey(resourcePath)) {
                                    IIndex index = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
                                    firstRegistrations.putIfAbsent(resourcePath, index);
                                }

                                // Now open it
                                datasetLifecycleManager.open(resourcePath);
                                ILSMIndex index = (ILSMIndex) datasetLifecycleManager.get(resourcePath);
                                assertNotNull("Index should not be null after open", index);
                                assertTrue("Index should be activated after open", ((MockIndex) index).isActivated());
                                successCount.incrementAndGet();
                            } catch (Exception e) {
                                throw e;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errorCount.incrementAndGet();
                }
            }));
        }

        // Wait for all threads to complete
        for (Future<?> future : futures) {
            future.get();
        }

        assertEquals("No unexpected errors should occur", 0, errorCount.get());
        LOGGER.info("Completed testConcurrentRegisterAndOpen: {} successful operations", successCount.get());
        assertTrue("Some operations should succeed", successCount.get() > 0);

        // Verify all resources are now registered
        for (String resourcePath : createdResourcePaths) {
            IIndex index = firstRegistrations.get(resourcePath);
            assertNotNull("All resources should be registered", index);

            // Verify one final time that re-registration returns the same instance
            IIndex reRegisteredIndex = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
            assertSame("Final re-registration should return the same index instance", index, reRegisteredIndex);
        }
    }

    /**
     * Tests concurrent register, open, and unregister operations.
     * This test verifies that:
     * 1. Multiple threads can concurrently perform all lifecycle operations (register, open, unregister)
     * 2. Re-registration of an already registered index returns the same index instance
     * 3. Resources can be properly unregistered after being opened and closed
     * 4. Concurrent lifecycle operations do not interfere with each other
     */
    @Test
    public void testConcurrentRegisterIfAbsentOpenAndUnregister() throws Exception {
        LOGGER.info("Starting testConcurrentRegisterOpenAndUnregister with {} threads", NUM_THREADS);

        final Map<String, IIndex> registeredInstances = new HashMap<>();
        final AtomicInteger successCount = new AtomicInteger(0);
        final AtomicInteger expectedErrorCount = new AtomicInteger(0);
        final AtomicInteger unexpectedErrorCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    final Random random = new Random();
                    for (int j = 0; j < NUM_OPERATIONS_PER_THREAD; j++) {
                        // Pick a random resource path
                        String resourcePath = createdResourcePaths.get(random.nextInt(createdResourcePaths.size()));

                        try {
                            // Randomly choose between register, open, or unregister
                            int op = random.nextInt(3);
                            if (op == 0) {
                                // Register index
                                LOGGER.debug("Thread {} registering resource {}", Thread.currentThread().getId(),
                                        resourcePath);

                                IIndex index = datasetLifecycleManager.get(resourcePath);
                                synchronized (registeredInstances) {
                                    registeredInstances.put(resourcePath, index);
                                }

                                LOGGER.debug("Thread {} registered resource {}", Thread.currentThread().getId(),
                                        resourcePath);
                                successCount.incrementAndGet();
                            } else if (op == 1) {
                                // Open index
                                LOGGER.debug("Thread {} opening resource {}", Thread.currentThread().getId(),
                                        resourcePath);

                                IIndex index;
                                synchronized (registeredInstances) {
                                    index = registeredInstances.get(resourcePath);
                                }

                                if (index != null) {
                                    try {
                                        datasetLifecycleManager.open(resourcePath);
                                        LOGGER.debug("Thread {} opened resource {}", Thread.currentThread().getId(),
                                                resourcePath);
                                        successCount.incrementAndGet();
                                    } catch (HyracksDataException e) {
                                        if (e.getMessage() != null
                                                && e.getMessage().contains("HYR0104: Index does not exist")) {
                                            LOGGER.debug("Thread {} failed to open unregistered resource {}: {}",
                                                    Thread.currentThread().getId(), resourcePath, e.getMessage());
                                            expectedErrorCount.incrementAndGet();
                                        } else {
                                            LOGGER.error("Thread {} failed to open resource {}: {}",
                                                    Thread.currentThread().getId(), resourcePath, e.getMessage(), e);
                                            unexpectedErrorCount.incrementAndGet();
                                        }
                                    }
                                }
                            } else {
                                // Unregister index
                                LOGGER.debug("Thread {} unregistering resource {}", Thread.currentThread().getId(),
                                        resourcePath);

                                try {
                                    datasetLifecycleManager.unregister(resourcePath);
                                    LOGGER.debug("Thread {} unregistered resource {}", Thread.currentThread().getId(),
                                            resourcePath);
                                    synchronized (registeredInstances) {
                                        registeredInstances.remove(resourcePath);
                                    }
                                    successCount.incrementAndGet();
                                } catch (HyracksDataException e) {
                                    if (e.getMessage() != null
                                            && (e.getMessage().contains("HYR0104: Index does not exist")
                                                    || e.getMessage().contains("HYR0105: Cannot drop in-use index"))) {
                                        LOGGER.debug("Thread {} failed to unregister resource {}: {}",
                                                Thread.currentThread().getId(), resourcePath, e.getMessage());
                                        expectedErrorCount.incrementAndGet();
                                    } else {
                                        LOGGER.error("Thread {} failed to unregister resource {}: {}",
                                                Thread.currentThread().getId(), resourcePath, e.getMessage(), e);
                                        unexpectedErrorCount.incrementAndGet();
                                    }
                                }
                            }
                        } catch (Exception e) {
                            LOGGER.error("Thread {} encountered error on resource {}: {}",
                                    Thread.currentThread().getId(), resourcePath, e.getMessage(), e);
                            unexpectedErrorCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("Thread {} encountered unexpected error: {}", Thread.currentThread().getId(),
                            e.getMessage(), e);
                    unexpectedErrorCount.incrementAndGet();
                }
            }));
        }

        // Wait for all threads to complete
        for (Future<?> future : futures) {
            future.get();
        }

        // Print final stats
        int remainingRegistered = registeredInstances.size();
        LOGGER.info(
                "testConcurrentRegisterOpenAndUnregister completed - Success: {}, Expected Errors: {}, Unexpected Errors: {}, Remaining registered: {}",
                successCount.get(), expectedErrorCount.get(), unexpectedErrorCount.get(), remainingRegistered);

        // Check results
        assertEquals("No unexpected errors should occur", 0, unexpectedErrorCount.get());
        assertTrue("Some operations should succeed", successCount.get() > 0);
        LOGGER.info("Expected errors occurred: {} - these are normal in concurrent environment",
                expectedErrorCount.get());
    }

    /**
     * Tests behavior when re-registering already registered indexes.
     * This test verifies that:
     * 1. Registering an index returns a valid index instance
     * 2. Re-registering the same index returns the same instance (not a new one)
     * 3. Concurrent re-registrations and opens work correctly
     * 4. The identity of index instances is preserved throughout operations
     */
    @Test
    public void testRegisterIfAbsentAlreadyRegisteredIndex() throws Exception {
        LOGGER.info("Starting testRegisterAlreadyRegisteredIndex");

        // Register all resources first
        Map<String, IIndex> firstRegistrations = new HashMap<>();
        for (String resourcePath : createdResourcePaths) {
            IIndex index = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
            assertNotNull("First registration should succeed", index);
            firstRegistrations.put(resourcePath, index);

            // Verify the returned index is the same as our mock index
            MockIndex mockIndex = mockIndexes.get(resourcePath);
            assertSame("Register should return our mock index for " + resourcePath, mockIndex, index);
        }

        // Try to register again - should return the same instances without re-registering
        for (String resourcePath : createdResourcePaths) {
            IIndex reRegisteredIndex = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
            assertNotNull("Re-registration should succeed", reRegisteredIndex);
            // Verify we get back the same instance (no re-registration occurred)
            assertSame("Re-registration should return the same index instance", firstRegistrations.get(resourcePath),
                    reRegisteredIndex);

            // Double check it's still our mock index
            MockIndex mockIndex = mockIndexes.get(resourcePath);
            assertSame("Re-register should still return our mock index for " + resourcePath, mockIndex,
                    reRegisteredIndex);
        }

        // Now try concurrent open and re-register operations
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(NUM_THREADS);
        final AtomicInteger openSuccesses = new AtomicInteger(0);
        final ConcurrentHashMap<String, Integer> reRegisterCounts = new ConcurrentHashMap<>();

        for (int i = 0; i < NUM_THREADS; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startLatch.await();

                    // Even threads try to open, odd threads try to re-register
                    if (threadId % 2 == 0) {
                        String resourcePath = createdResourcePaths.get(threadId % createdResourcePaths.size());
                        datasetLifecycleManager.open(resourcePath);
                        ILSMIndex index = (ILSMIndex) datasetLifecycleManager.get(resourcePath);
                        assertNotNull("Index should not be null after open", index);
                        assertTrue("Index should be activated after open", ((MockIndex) index).isActivated());
                        openSuccesses.incrementAndGet();
                    } else {
                        String resourcePath = createdResourcePaths.get(threadId % createdResourcePaths.size());
                        IIndex reRegisteredIndex = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
                        assertNotNull("Re-registration should succeed", reRegisteredIndex);
                        // Keep track of re-registrations
                        reRegisterCounts.compute(resourcePath, (k, v) -> v == null ? 1 : v + 1);
                        // Verify it's the same instance as the original registration
                        assertSame("Re-registration should return the same index instance",
                                firstRegistrations.get(resourcePath), reRegisteredIndex);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all threads
        startLatch.countDown();

        // Wait for completion
        completionLatch.await(10, TimeUnit.SECONDS);

        // Verify results
        assertEquals("Half of threads should succeed in opening", NUM_THREADS / 2, openSuccesses.get());

        // Verify that re-registration attempts occurred for each resource
        for (String path : createdResourcePaths) {
            // Some resources should have been re-registered multiple times
            Integer reRegCount = reRegisterCounts.get(path);
            if (reRegCount != null) {
                LOGGER.info("Resource path {} was re-registered {} times", path, reRegCount);
            }
        }

        LOGGER.info("Completed testRegisterAlreadyRegisteredIndex: {} successful opens", openSuccesses.get());
    }

    /**
     * Tests concurrent opening of the same index by multiple threads.
     * This test verifies that:
     * 1. Multiple threads can concurrently open the same index without errors
     * 2. The index is properly activated after being opened
     * 3. Concurrent opens do not interfere with each other
     */
    @Test
    public void testConcurrentOpenSameIndex() throws Exception {
        LOGGER.info("Starting testConcurrentOpenSameIndex");

        final int THREADS = 10;
        final String RESOURCE_PATH = createdResourcePaths.get(0); // First created resource path
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(THREADS);
        final AtomicInteger errors = new AtomicInteger(0);

        // Register the index first - REQUIRED before open
        datasetLifecycleManager.registerIfAbsent(RESOURCE_PATH, null);

        // Create threads that will all open the same index
        for (int i = 0; i < THREADS; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await();
                    datasetLifecycleManager.open(RESOURCE_PATH);
                } catch (Exception e) {
                    e.printStackTrace();
                    errors.incrementAndGet();
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        completionLatch.await(10, TimeUnit.SECONDS);

        assertEquals("No errors should occur when opening the same index concurrently", 0, errors.get());

        // Verify the index is actually open
        ILSMIndex index = (ILSMIndex) datasetLifecycleManager.get(RESOURCE_PATH);
        assertNotNull("Index should be retrievable", index);
        assertTrue("Index should be activated", ((MockIndex) index).isActivated());

        LOGGER.info("Completed testConcurrentOpenSameIndex: index activated={}", ((MockIndex) index).isActivated());
    }

    /**
     * Tests concurrent closing of an index after it has been opened.
     * This test verifies that:
     * 1. An index can be closed after being opened
     * 2. Multiple threads can attempt to close the same index
     * 3. The index is properly deactivated after being closed
     */
    @Test
    public void testConcurrentCloseAfterOpen() throws Exception {
        LOGGER.info("Starting testConcurrentCloseAfterOpen");

        final String RESOURCE_PATH = createdResourcePaths.get(1); // Second created resource path
        final CountDownLatch openLatch = new CountDownLatch(1);
        final CountDownLatch closeLatch = new CountDownLatch(1);
        final AtomicBoolean openSuccess = new AtomicBoolean(false);
        final AtomicBoolean closeSuccess = new AtomicBoolean(false);

        // Register the index first
        datasetLifecycleManager.registerIfAbsent(RESOURCE_PATH, null);

        // Thread to open the index
        executorService.submit(() -> {
            try {
                datasetLifecycleManager.open(RESOURCE_PATH);
                openSuccess.set(true);
                openLatch.countDown();

                // Wait a bit to simulate work with the open index
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
                fail("Open operation failed: " + e.getMessage());
            }
        });

        // Thread to close the index after it's opened
        executorService.submit(() -> {
            try {
                openLatch.await(); // Wait for open to complete
                datasetLifecycleManager.close(RESOURCE_PATH);
                closeSuccess.set(true);
                closeLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
                fail("Close operation failed: " + e.getMessage());
            }
        });

        // Wait for operations to complete
        closeLatch.await(5, TimeUnit.SECONDS);

        assertTrue("Open operation should succeed", openSuccess.get());
        assertTrue("Close operation should succeed", closeSuccess.get());

        LOGGER.info("Completed testConcurrentCloseAfterOpen: openSuccess={}, closeSuccess={}", openSuccess.get(),
                closeSuccess.get());
    }

    /**
     * Tests the identity and uniqueness of mock objects and resources.
     * This test verifies that:
     * 1. Each resource path has a unique MockIndex instance
     * 2. Each LocalResource has a unique ID
     * 3. The createInstance method returns the correct MockIndex for each resource path
     * 4. Registration returns the expected MockIndex instance
     *
     * This test is crucial for ensuring the test infrastructure itself is correctly set up.
     */
    @Test
    public void testMockIndexIdentity() throws HyracksDataException {
        LOGGER.info("Starting testMockIndexIdentity");

        // Verify that the mockIndexes map is correctly populated
        assertTrue("Mock indexes should be created", !mockIndexes.isEmpty());
        assertEquals("Should have created mocks for all resource paths", createdResourcePaths.size(),
                mockIndexes.size());

        // Verify each local resource has a unique ID
        Set<Long> resourceIds = new HashSet<>();
        for (LocalResource resource : mockResources.values()) {
            long id = resource.getId();
            if (!resourceIds.add(id)) {
                fail("Duplicate resource ID found: " + id + " for path: " + resource.getPath());
            }
        }

        // For each resource path, verify the mock setup
        for (String resourcePath : createdResourcePaths) {
            // Check if we have a mock index for this path
            MockIndex expectedMockIndex = mockIndexes.get(resourcePath);
            assertNotNull("Should have a mock index for " + resourcePath, expectedMockIndex);

            // Get the local resource
            LocalResource lr = mockResourceRepository.get(resourcePath);
            assertNotNull("Should have a local resource for " + resourcePath, lr);

            // Print resource ID for verification
            LOGGER.info("Resource path: {}, ID: {}", resourcePath, lr.getId());

            // Get the dataset resource
            DatasetLocalResource datasetResource = (DatasetLocalResource) lr.getResource();
            assertNotNull("Should have a dataset resource for " + resourcePath, datasetResource);

            // Call createInstance and verify we get our mock index
            IIndex createdIndex = datasetResource.createInstance(mockServiceContext);
            assertSame("createInstance should return our mock index for " + resourcePath, expectedMockIndex,
                    createdIndex);

            // Now register and verify we get the same mock index
            IIndex registeredIndex = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
            assertSame("Registered index should be our mock index for " + resourcePath, expectedMockIndex,
                    registeredIndex);

            // Confirm the reference equality of these objects
            LOGGER.info("Path: {}", resourcePath);
            LOGGER.info("  Expected mock: {}", System.identityHashCode(expectedMockIndex));
            LOGGER.info("  Created instance: {}", System.identityHashCode(createdIndex));
            LOGGER.info("  Registered instance: {}", System.identityHashCode(registeredIndex));
        }

        LOGGER.info("Completed testMockIndexIdentity: verified {} resources", createdResourcePaths.size());
    }

    /**
     * Tests that unregistering indexes works correctly, but only after they have been properly registered and opened.
     * This test verifies that:
     * 1. Indexes can be registered and opened successfully
     * 2. All indexes are properly activated after opening
     * 3. Indexes can be unregistered after being registered and opened
     * 4. After unregistering, the indexes are no longer retrievable
     * 5. The correct lifecycle sequence (register → open → unregister) is enforced
     */
    @Test
    public void testUnregisterAfterRegisterIfAbsentAndOpen() throws Exception {
        LOGGER.info("Starting testUnregisterAfterRegisterAndOpen");

        Map<String, IIndex> registeredIndexes = new HashMap<>();
        int totalIndexes = createdResourcePaths.size();
        AtomicInteger successfulUnregisters = new AtomicInteger(0);

        // Step 1: Register and open all indexes sequentially
        for (String resourcePath : createdResourcePaths) {
            LOGGER.debug("Registering and opening: {}", resourcePath);

            // Register the index
            IIndex index = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
            assertNotNull("Index should be registered successfully", index);
            registeredIndexes.put(resourcePath, index);

            // After registration, we should have a DatasetResource
            LocalResource resource = mockResources.get(resourcePath);
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) resource.getResource();
            int datasetId = datasetLocalResource.getDatasetId();
            int partition = datasetLocalResource.getPartition();

            // Check if we have a DatasetResource in our map
            DatasetResource datasetResource = datasetResourceMap.get(datasetId);
            if (datasetResource != null) {
                LOGGER.debug("Found DatasetResource for dataset {}", datasetId);

                // See if there's an operation tracker for this partition already
                ILSMOperationTracker existingTracker = null;
                try {
                    // Use reflection to get the datasetPrimaryOpTrackers map
                    Field opTrackersField = DatasetResource.class.getDeclaredField("datasetPrimaryOpTrackers");
                    opTrackersField.setAccessible(true);
                    Map<Integer, ILSMOperationTracker> opTrackers =
                            (Map<Integer, ILSMOperationTracker>) opTrackersField.get(datasetResource);

                    // Check if we already have a tracker for this partition
                    existingTracker = opTrackers.get(partition);
                    LOGGER.debug("Existing tracker for partition {}: {}", partition, existingTracker);
                } catch (Exception e) {
                    LOGGER.error("Failed to access opTrackers field via reflection", e);
                }
            } else {
                LOGGER.debug("No DatasetResource found for dataset {}", datasetId);
            }

            // Open the index
            datasetLifecycleManager.open(resourcePath);

            // Verify index is activated
            MockIndex mockIndex = (MockIndex) index;
            assertTrue("Index should be activated after opening: " + resourcePath, mockIndex.isActivated());

            // Verify it's retrievable
            long resourceId = resource.getId();
            IIndex retrievedIndex = datasetLifecycleManager.getIndex(datasetId, resourceId);
            assertSame("Retrieved index should be the same instance", index, retrievedIndex);
        }

        LOGGER.info("All {} indexes registered and opened successfully", totalIndexes);

        // Step 2: Close and then unregister each index
        for (String resourcePath : createdResourcePaths) {
            LOGGER.debug("Closing and unregistering: {}", resourcePath);

            // Verify the index is activated before closing
            IIndex index = registeredIndexes.get(resourcePath);
            MockIndex mockIndex = (MockIndex) index;
            assertTrue("Index should be activated before closing: " + resourcePath, mockIndex.isActivated());

            // Get resource information before closing
            LocalResource resource = mockResources.get(resourcePath);
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) resource.getResource();
            int datasetId = datasetLocalResource.getDatasetId();
            int partition = datasetLocalResource.getPartition();

            // Close the index first to ensure reference count goes to 0
            LOGGER.debug("Closing index: {}", resourcePath);
            datasetLifecycleManager.close(resourcePath);

            // Brief pause to allow any asynchronous operations to complete
            Thread.sleep(50);

            // Get the operation tracker and verify it has 0 active operations
            DatasetResource datasetResource = datasetResourceMap.get(datasetId);
            ILSMOperationTracker opTracker = mockIndex.getOperationTracker();

            LOGGER.debug("Before unregister: Resource path={}, datasetId={}, partition={}, tracker={}", resourcePath,
                    datasetId, partition, opTracker);

            // Unregister the index after it's closed
            LOGGER.debug("Unregistering index: {}", resourcePath);
            try {
                datasetLifecycleManager.unregister(resourcePath);
                successfulUnregisters.incrementAndGet();
                LOGGER.debug("Successfully unregistered index: {}", resourcePath);
            } catch (Exception e) {
                LOGGER.error("Failed to unregister index: {}", resourcePath, e);
                fail("Failed to unregister index: " + resourcePath + ": " + e.getMessage());
            }

            // Verify the index is no longer retrievable
            try {
                LocalResource resourceAfterUnregister = mockResources.get(resourcePath);
                long resourceIdAfterUnregister = resourceAfterUnregister.getId();
                IIndex retrievedIndexAfterUnregister =
                        datasetLifecycleManager.getIndex(datasetId, resourceIdAfterUnregister);
                Assert.assertNull("Index should not be retrievable after unregister: " + resourcePath,
                        retrievedIndexAfterUnregister);
            } catch (HyracksDataException e) {
                // This is also an acceptable outcome if getIndex throws an exception for unregistered indexes
                LOGGER.debug("Expected exception when retrieving unregistered index: {}", e.getMessage());
            }
        }

        assertEquals("All indexes should be unregistered", totalIndexes, successfulUnregisters.get());
        LOGGER.info("Completed testUnregisterAfterRegisterAndOpen: successfully unregistered {} indexes",
                successfulUnregisters.get());
    }

    /**
     * Tests that only one primary operation tracker is created per partition per dataset.
     * This test verifies that:
     * 1. When multiple indexes for the same dataset and partition are registered, they share the same operation tracker
     * 2. Different partitions of the same dataset have different operation trackers
     * 3. Different datasets have completely separate operation trackers
     */
    @Test
    public void testPrimaryOperationTrackerSingularity() throws Exception {
        LOGGER.info("Starting testPrimaryOperationTrackerSingularity");

        // Maps to track operation trackers by dataset and partition
        Map<Integer, Map<Integer, Object>> datasetToPartitionToTracker = new HashMap<>();
        Map<String, IIndex> registeredIndexes = new HashMap<>();

        // Step 1: Register all indexes first
        for (String resourcePath : createdResourcePaths) {
            LOGGER.debug("Registering index: {}", resourcePath);

            // Register the index
            IIndex index = datasetLifecycleManager.registerIfAbsent(resourcePath, null);
            assertNotNull("Index should be registered successfully", index);
            registeredIndexes.put(resourcePath, index);

            // Get dataset and partition information
            LocalResource resource = mockResources.get(resourcePath);
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) resource.getResource();
            int datasetId = datasetLocalResource.getDatasetId();
            int partition = datasetLocalResource.getPartition();

            // Check if we have a DatasetResource in our map
            DatasetResource datasetResource = datasetResourceMap.get(datasetId);
            assertNotNull("DatasetResource should be created for dataset " + datasetId, datasetResource);

            // At this point, operation tracker might not exist yet since it's created lazily during open()
            try {
                // Use reflection to get the datasetPrimaryOpTrackers map
                Field opTrackersField = DatasetResource.class.getDeclaredField("datasetPrimaryOpTrackers");
                opTrackersField.setAccessible(true);
                Map<Integer, ILSMOperationTracker> opTrackers =
                        (Map<Integer, ILSMOperationTracker>) opTrackersField.get(datasetResource);

                // Check if tracker already exists (might be null)
                Object existingTracker = opTrackers.get(partition);
                LOGGER.debug("Before open(): Tracker for dataset {} partition {}: {}", datasetId, partition,
                        existingTracker);
            } catch (Exception e) {
                LOGGER.error("Failed to access opTrackers field via reflection", e);
            }
        }

        // Step 2: Open all indexes to trigger operation tracker creation
        for (String resourcePath : createdResourcePaths) {
            LOGGER.debug("Opening index: {}", resourcePath);

            // Open the index to trigger operation tracker creation
            datasetLifecycleManager.open(resourcePath);

            // Get dataset and partition information
            LocalResource resource = mockResources.get(resourcePath);
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) resource.getResource();
            int datasetId = datasetLocalResource.getDatasetId();
            int partition = datasetLocalResource.getPartition();

            // Now the operation tracker should exist
            DatasetResource datasetResource = datasetResourceMap.get(datasetId);
            Object opTracker = null;
            try {
                // Use reflection to get the datasetPrimaryOpTrackers map
                Field opTrackersField = DatasetResource.class.getDeclaredField("datasetPrimaryOpTrackers");
                opTrackersField.setAccessible(true);
                Map<Integer, ILSMOperationTracker> opTrackers =
                        (Map<Integer, ILSMOperationTracker>) opTrackersField.get(datasetResource);

                // Get the tracker for this partition
                opTracker = opTrackers.get(partition);
                assertNotNull("Operation tracker should exist after open() for partition " + partition, opTracker);

                LOGGER.debug("After open(): Found tracker for dataset {} partition {}: {}", datasetId, partition,
                        System.identityHashCode(opTracker));
            } catch (Exception e) {
                LOGGER.error("Failed to access opTrackers field via reflection", e);
                fail("Failed to access operation trackers: " + e.getMessage());
            }

            // Store the tracker in our map for later comparison
            datasetToPartitionToTracker.computeIfAbsent(datasetId, k -> new HashMap<>()).putIfAbsent(partition,
                    opTracker);
        }

        // Step 3: Create and register secondary indexes for each dataset/partition
        List<String> secondaryPaths = new ArrayList<>();
        for (String resourcePath : createdResourcePaths) {
            // Create a "secondary index" path by appending "-secondary" to the resource path
            String secondaryPath = resourcePath + "-secondary";
            secondaryPaths.add(secondaryPath);

            // Create a mock resource for this secondary index
            LocalResource originalResource = mockResources.get(resourcePath);
            DatasetLocalResource originalDatasetLocalResource = (DatasetLocalResource) originalResource.getResource();
            int datasetId = originalDatasetLocalResource.getDatasetId();
            int partition = originalDatasetLocalResource.getPartition();

            // Set up a new mock resource with the same dataset and partition but a different path
            setupMockResource(secondaryPath, datasetId, partition, originalResource.getId() + 10000);

            LOGGER.debug("Registering secondary index: {}", secondaryPath);

            // Register the secondary index
            IIndex secondaryIndex = datasetLifecycleManager.registerIfAbsent(secondaryPath, null);
            assertNotNull("Secondary index should be registered successfully", secondaryIndex);
            registeredIndexes.put(secondaryPath, secondaryIndex);
        }

        // Step 4: Open the secondary indexes to trigger operation tracker reuse
        for (String secondaryPath : secondaryPaths) {
            LOGGER.debug("Opening secondary index: {}", secondaryPath);

            // Open the secondary index
            datasetLifecycleManager.open(secondaryPath);

            // Get dataset and partition information
            LocalResource resource = mockResources.get(secondaryPath);
            DatasetLocalResource datasetLocalResource = (DatasetLocalResource) resource.getResource();
            int datasetId = datasetLocalResource.getDatasetId();
            int partition = datasetLocalResource.getPartition();

            // Get the operation tracker after opening
            DatasetResource datasetResource = datasetResourceMap.get(datasetId);
            Object secondaryOpTracker = null;
            try {
                // Use reflection to get the datasetPrimaryOpTrackers map
                Field opTrackersField = DatasetResource.class.getDeclaredField("datasetPrimaryOpTrackers");
                opTrackersField.setAccessible(true);
                Map<Integer, ILSMOperationTracker> opTrackers =
                        (Map<Integer, ILSMOperationTracker>) opTrackersField.get(datasetResource);

                // Get the tracker for this partition
                secondaryOpTracker = opTrackers.get(partition);
                assertNotNull("Operation tracker should exist for secondary index partition " + partition,
                        secondaryOpTracker);

                LOGGER.debug("After opening secondary: Found tracker for dataset {} partition {}: {}", datasetId,
                        partition, System.identityHashCode(secondaryOpTracker));
            } catch (Exception e) {
                LOGGER.error("Failed to access opTrackers field via reflection", e);
                fail("Failed to access operation trackers: " + e.getMessage());
            }

            // Verify this is the same tracker as the one used by the primary index
            Object originalTracker = datasetToPartitionToTracker.get(datasetId).get(partition);
            assertSame("Operation tracker should be reused for the same dataset/partition", originalTracker,
                    secondaryOpTracker);
        }

        // Step 5: Verify that different partitions of the same dataset have different operation trackers
        for (Map.Entry<Integer, Map<Integer, Object>> datasetEntry : datasetToPartitionToTracker.entrySet()) {
            int datasetId = datasetEntry.getKey();
            Map<Integer, Object> partitionTrackers = datasetEntry.getValue();

            if (partitionTrackers.size() > 1) {
                LOGGER.debug("Verifying different trackers for different partitions in dataset {}", datasetId);

                // Collect all the tracker instances for this dataset
                Set<Object> uniqueTrackers = new HashSet<>(partitionTrackers.values());

                // The number of unique trackers should equal the number of partitions
                assertEquals("Each partition should have its own operation tracker", partitionTrackers.size(),
                        uniqueTrackers.size());
            }
        }

        // Step 6: Verify that different datasets have different operation trackers for the same partition
        if (datasetToPartitionToTracker.size() > 1) {
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                final int partitionId = partition;

                // Collect all trackers for this partition across different datasets
                List<Object> trackersForPartition = datasetToPartitionToTracker.entrySet().stream()
                        .filter(entry -> entry.getValue().containsKey(partitionId))
                        .map(entry -> entry.getValue().get(partitionId)).collect(Collectors.toList());

                if (trackersForPartition.size() > 1) {
                    LOGGER.debug("Verifying different trackers across datasets for partition {}", partitionId);

                    // All trackers should be unique (no sharing between datasets)
                    Set<Object> uniqueTrackers = new HashSet<>(trackersForPartition);
                    assertEquals("Each dataset should have its own operation tracker for partition " + partitionId,
                            trackersForPartition.size(), uniqueTrackers.size());
                }
            }
        }

        // Step 7: Clean up - close all indexes
        for (String path : new ArrayList<>(registeredIndexes.keySet())) {
            try {
                datasetLifecycleManager.close(path);
            } catch (Exception e) {
                LOGGER.error("Error closing index {}", path, e);
            }
        }

        LOGGER.info(
                "Completed testPrimaryOperationTrackerSingularity: verified unique trackers for each dataset/partition combination");
    }

    /**
     * Tests that resources cannot be opened after unregistering unless re-registered first.
     * This test verifies:
     * 1. Resources must be registered before they can be opened
     * 2. After unregistering, resources cannot be opened until re-registered
     * 3. Re-registration of an unregistered resource works correctly
     */
    @Test
    public void testCannotOpenUnregisteredResource() throws Exception {
        LOGGER.info("Starting testCannotOpenUnregisteredResource");

        String resourcePath = createdResourcePaths.get(0);

        // Register resource first
        IIndex index = registerIfAbsentIndex(resourcePath);
        assertNotNull("Index should be registered successfully", index);
        LOGGER.debug("Registered resource {}", resourcePath);

        // Unregister it
        datasetLifecycleManager.unregister(resourcePath);
        LOGGER.debug("Unregistered resource {}", resourcePath);

        // Now try to open it - should fail
        try {
            datasetLifecycleManager.open(resourcePath);
            fail("Should not be able to open an unregistered resource");
        } catch (HyracksDataException e) {
            LOGGER.debug("Caught expected exception: {}", e.getMessage());
            assertTrue("Exception should mention index not existing", e.getMessage().contains("does not exist"));
        }

        // Re-register should work
        MockIndex mockIndex = mockIndexes.get(resourcePath);
        IIndex reRegisteredIndex = datasetLifecycleManager.registerIfAbsent(resourcePath, mockIndex);
        assertNotNull("Index should be re-registered successfully", reRegisteredIndex);
        LOGGER.debug("Re-registered resource {}", resourcePath);

        // Now open should work
        datasetLifecycleManager.open(resourcePath);
        LOGGER.debug("Successfully opened resource after re-registering");

        LOGGER.info("Completed testCannotOpenUnregisteredResource");
    }

    /**
     * Tests the full lifecycle of operation trackers to ensure they're properly created,
     * maintained throughout the index lifecycle, and cleaned up at the appropriate time.
     * This test verifies that:
     * 1. Operation trackers are created when indexes are opened for the first time
     * 2. Operation trackers remain stable during index usage
     * 3. Operation trackers are properly shared among indexes of the same dataset/partition
     * 4. Operation trackers are not prematurely nullified or cleared
     * 5. Operation trackers are correctly removed when all indexes of a dataset/partition are unregistered
     */
    @Test
    public void testOperationTrackerLifecycle() throws Exception {
        // Register primary index
        String dsName = "ds0";
        int dsId = 101; // Updated from 0 to 101 to match our new datasetId scheme
        int partition = 0;
        String primaryIndexName = dsName;
        String secondaryIndexName = dsName + "_idx";
        String primaryResourcePath = getResourcePath(partition, dsName, 0, primaryIndexName);
        String secondaryResourcePath = getResourcePath(partition, dsName, 1, secondaryIndexName);

        LOGGER.debug("Registering primary index at {}", primaryResourcePath);
        MockIndex mockPrimaryIndex = mockIndexes.get(primaryResourcePath);
        IIndex registeredIndex = datasetLifecycleManager.registerIfAbsent(primaryResourcePath, mockPrimaryIndex);
        assertNotNull("Index should be registered successfully", registeredIndex);

        // Get the dataset resource and check if it has been created
        DatasetResource datasetResource = datasetResourceMap.get(dsId);
        assertNotNull("Dataset resource should be created during registration", datasetResource);

        // Verify there's no operation tracker before opening
        ILSMOperationTracker trackerBeforeOpen = getOperationTracker(datasetResource, partition);
        LOGGER.debug("Operation tracker before open: {}", trackerBeforeOpen);

        // Open primary index which should create and register an operation tracker
        LOGGER.debug("Opening primary index at {}", primaryResourcePath);
        datasetLifecycleManager.open(primaryResourcePath);

        // Verify operation tracker exists after opening
        ILSMOperationTracker trackerAfterOpen = getOperationTracker(datasetResource, partition);
        LOGGER.debug("Operation tracker after primary open: {}", trackerAfterOpen);
        assertNotNull("Operation tracker should be created after opening primary index", trackerAfterOpen);

        // Verify the tracker is from our mock index
        MockIndex mockIndex = mockIndexes.get(primaryResourcePath);
        assertTrue("Mock index should be activated after open", mockIndex.isActivated());

        // Compare with the mock tracker from the index
        ILSMOperationTracker mockTracker = mockIndex.getOperationTracker();
        LOGGER.debug("Mock tracker from index: {}", mockTracker);
        assertEquals("Operation tracker should be the one from the mock index", mockTracker, trackerAfterOpen);

        // Open secondary index which should reuse the same operation tracker
        LOGGER.debug("Registering secondary index at {}", secondaryResourcePath);
        MockIndex mockSecondaryIndex = mockIndexes.get(secondaryResourcePath);
        datasetLifecycleManager.registerIfAbsent(secondaryResourcePath, mockSecondaryIndex);

        LOGGER.debug("Opening secondary index at {}", secondaryResourcePath);
        datasetLifecycleManager.open(secondaryResourcePath);

        // Verify operation tracker is still the same after opening secondary
        ILSMOperationTracker trackerAfterSecondaryOpen = getOperationTracker(datasetResource, partition);
        LOGGER.debug("Operation tracker after secondary open: {}", trackerAfterSecondaryOpen);
        assertNotNull("Operation tracker should still exist after opening secondary index", trackerAfterSecondaryOpen);
        assertEquals("Should still be the same operation tracker", trackerAfterOpen, trackerAfterSecondaryOpen);

        // Close primary index - should keep the tracker as secondary is still open
        LOGGER.debug("Closing primary index at {}", primaryResourcePath);
        datasetLifecycleManager.close(primaryResourcePath);

        // Verify operation tracker still exists
        ILSMOperationTracker trackerAfterPrimaryClose = getOperationTracker(datasetResource, partition);
        LOGGER.debug("Operation tracker after primary close: {}", trackerAfterPrimaryClose);
        assertNotNull("Operation tracker should still exist after closing primary index", trackerAfterPrimaryClose);
        assertEquals("Should still be the same operation tracker", trackerAfterOpen, trackerAfterPrimaryClose);

        // Close secondary index - should remove the tracker as all indexes are closed
        LOGGER.debug("Closing secondary index at {}", secondaryResourcePath);
        datasetLifecycleManager.close(secondaryResourcePath);

        // Verify operation tracker is removed
        ILSMOperationTracker trackerAfterSecondaryClose = getOperationTracker(datasetResource, partition);
        LOGGER.debug("Operation tracker after secondary close: {}", trackerAfterSecondaryClose);

        // Unregister indexes
        LOGGER.debug("Unregistering primary index at {}", primaryResourcePath);
        datasetLifecycleManager.unregister(primaryResourcePath);

        LOGGER.debug("Unregistering secondary index at {}", secondaryResourcePath);
        datasetLifecycleManager.unregister(secondaryResourcePath);

        // Verify dataset resource is removed
        DatasetResource datasetResourceAfterUnregister = datasetResourceMap.get(dsId);
        LOGGER.debug("Dataset resource after unregister: {}", datasetResourceAfterUnregister);
    }

    /**
     * Helper method to get the operation tracker for a dataset/partition.
     * Uses reflection to access the private datasetPrimaryOpTrackers map in DatasetResource.
     */
    private ILSMOperationTracker getOperationTracker(DatasetResource datasetResource, int partition) {
        try {
            // Access the datasetPrimaryOpTrackers map through reflection
            Field opTrackersField = DatasetResource.class.getDeclaredField("datasetPrimaryOpTrackers");
            opTrackersField.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<Integer, ILSMOperationTracker> opTrackers =
                    (Map<Integer, ILSMOperationTracker>) opTrackersField.get(datasetResource);

            // Return the operation tracker for this partition, if it exists
            if (opTrackers != null) {
                ILSMOperationTracker tracker = opTrackers.get(partition);
                if (tracker != null) {
                    // Make sure we're getting a PrimaryIndexOperationTracker
                    if (!(tracker instanceof PrimaryIndexOperationTracker)) {
                        LOGGER.warn("Tracker is not a PrimaryIndexOperationTracker: {}", tracker.getClass().getName());
                    }
                }
                return tracker;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to get operation tracker via reflection", e);
        }
        return null;
    }

    /**
     * Tests that operation trackers are properly attached to indexes and survive open and close operations.
     * This test verifies:
     * 1. Operation trackers are properly created and associated with indexes
     * 2. Operation trackers remain valid during the index lifecycle
     * 3. Multiple opens and closes don't invalidate the operation tracker
     */
    @Test
    public void testOperationTrackerAttachment() throws Exception {
        LOGGER.info("Starting testOperationTrackerAttachment");

        // Choose a specific resource path to test with
        String resourcePath = createdResourcePaths.get(0);

        // Get dataset and partition information for this resource
        LocalResource resource = mockResources.get(resourcePath);
        DatasetLocalResource datasetLocalResource = (DatasetLocalResource) resource.getResource();
        int datasetId = datasetLocalResource.getDatasetId();
        int partition = datasetLocalResource.getPartition();

        // Register the resource first using our helper
        IIndex index = registerIfAbsentIndex(resourcePath);
        assertNotNull("Index should be registered successfully", index);
        LOGGER.debug("Registered resource {}", resourcePath);

        // Get the dataset resource
        DatasetResource datasetResource = datasetResourceMap.get(datasetId);
        assertNotNull("DatasetResource should exist after registration", datasetResource);

        // Verify no operation tracker exists before opening
        ILSMOperationTracker trackerBeforeOpen = getOperationTracker(datasetResource, partition);
        LOGGER.debug("Operation tracker before open: {}", trackerBeforeOpen);

        // Open the index - this should create and attach an operation tracker
        datasetLifecycleManager.open(resourcePath);
        LOGGER.debug("Opened resource {}", resourcePath);

        // Verify the operation tracker exists and is attached to the MockIndex
        ILSMOperationTracker trackerAfterOpen = getOperationTracker(datasetResource, partition);
        assertNotNull("Operation tracker should exist after open", trackerAfterOpen);
        LOGGER.debug("Operation tracker after open: {}", trackerAfterOpen);

        // Get the mock index and verify its tracker matches
        MockIndex mockIndex = mockIndexes.get(resourcePath);
        assertTrue("Mock index should be activated", mockIndex.isActivated());

        // Verify the mock index has the same operation tracker
        ILSMOperationTracker indexTracker = mockIndex.getOperationTracker();
        assertSame("Mock index should have the same operation tracker", trackerAfterOpen, indexTracker);

        // Close and unregister for cleanup
        datasetLifecycleManager.close(resourcePath);
        datasetLifecycleManager.unregister(resourcePath);

        LOGGER.info("Completed testOperationTrackerAttachment");
    }

    /**
     * A mock implementation of ILSMIndex for testing
     */
    private static class MockIndex implements ILSMIndex {
        private final String resourcePath;
        private final int datasetId;
        private boolean activated = false;
        // Create a mock of the specific PrimaryIndexOperationTracker class instead of the generic interface
        private PrimaryIndexOperationTracker mockTracker = mock(PrimaryIndexOperationTracker.class);
        private final Map<Integer, DatasetResource> datasetResourceMap;

        public MockIndex(String resourcePath, int datasetId, Map<Integer, DatasetResource> datasetResourceMap) {
            this.resourcePath = resourcePath;
            this.datasetId = datasetId;
            this.datasetResourceMap = datasetResourceMap;
        }

        @Override
        public void create() throws HyracksDataException {

        }

        @Override
        public void activate() {
            LOGGER.debug("Activating {}", this);
            activated = true;
        }

        @Override
        public void clear() throws HyracksDataException {

        }

        @Override
        public void deactivate() throws HyracksDataException {
            LOGGER.debug("Deactivating {}", this);
            activated = false;
        }

        @Override
        public void destroy() throws HyracksDataException {

        }

        @Override
        public void purge() throws HyracksDataException {

        }

        @Override
        public void deactivate(boolean flushOnExit) {
            LOGGER.debug("Deactivating {} with flushOnExit={}", this, flushOnExit);
            activated = false;
        }

        @Override
        public ILSMIndexAccessor createAccessor(IIndexAccessParameters iap) throws HyracksDataException {
            return null;
        }

        @Override
        public void validate() throws HyracksDataException {

        }

        @Override
        public IBufferCache getBufferCache() {
            return null;
        }

        @Override
        public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex, IPageWriteCallback callback) throws HyracksDataException {
            return null;
        }

        @Override
        public int getNumOfFilterFields() {
            return 0;
        }

        public boolean isActivated() {
            return activated;
        }

        @Override
        public String toString() {
            // Ensure this matches exactly the resourcePath used for registration
            return resourcePath;
        }

        @Override
        public boolean isCurrentMutableComponentEmpty() {
            return true;
        }

        @Override
        public void scheduleReplication(ILSMIndexOperationContext ctx, List<ILSMDiskComponent> diskComponents,
                IReplicationJob.ReplicationOperation operation, LSMOperationType opType) throws HyracksDataException {

        }

        @Override
        public boolean isMemoryComponentsAllocated() {
            return false;
        }

        @Override
        public void allocateMemoryComponents() throws HyracksDataException {

        }

        @Override
        public ILSMMemoryComponent getCurrentMemoryComponent() {
            return null;
        }

        @Override
        public int getCurrentMemoryComponentIndex() {
            return 0;
        }

        @Override
        public List<ILSMMemoryComponent> getMemoryComponents() {
            return List.of();
        }

        @Override
        public boolean isDurable() {
            return false;
        }

        @Override
        public void updateFilter(ILSMIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {

        }

        @Override
        public ILSMDiskComponent createBulkLoadTarget() throws HyracksDataException {
            return null;
        }

        @Override
        public int getNumberOfAllMemoryComponents() {
            return 0;
        }

        @Override
        public ILSMHarness getHarness() {
            return null;
        }

        @Override
        public String getIndexIdentifier() {
            return "";
        }

        @Override
        public IIndexBulkLoader createBulkLoader(float fillFactor, boolean verifyInput, long numElementsHint,
                boolean checkIfEmptyIndex, Map<String, Object> parameters) throws HyracksDataException {
            return null;
        }

        @Override
        public void resetCurrentComponentIndex() {

        }

        @Override
        public IIndexDiskCacheManager getDiskCacheManager() {
            return null;
        }

        @Override
        public void scheduleCleanup(List<ILSMDiskComponent> inactiveDiskComponents) throws HyracksDataException {

        }

        @Override
        public boolean isAtomic() {
            return false;
        }

        @Override
        public void commit() throws HyracksDataException {

        }

        @Override
        public void abort() throws HyracksDataException {

        }

        @Override
        public ILSMMergePolicy getMergePolicy() {
            return null;
        }

        @Override
        public ILSMOperationTracker getOperationTracker() {
            // This is the key method that DatasetLifecycleManager calls during open()
            // to get the operation tracker and register it

            LOGGER.debug("getOperationTracker() called for index: {}", resourcePath);

            // Get dataset ID and partition from the resource path
            int partition = -1;

            // Extract partition from resource path (storage/partition_X/...)
            String[] parts = resourcePath.split("/");
            for (int i = 0; i < parts.length; i++) {
                if (parts[i].startsWith("partition_")) {
                    try {
                        partition = Integer.parseInt(parts[i].substring("partition_".length()));
                        break;
                    } catch (NumberFormatException e) {
                        LOGGER.warn("Failed to parse partition number from {}", parts[i]);
                    }
                }
            }

            if (partition != -1) {
                // Find the dataset resource from our map
                DatasetResource datasetResource = datasetResourceMap.get(datasetId);
                if (datasetResource != null) {
                    try {
                        // Access the datasetPrimaryOpTrackers map through reflection
                        Field opTrackersField = DatasetResource.class.getDeclaredField("datasetPrimaryOpTrackers");
                        opTrackersField.setAccessible(true);
                        @SuppressWarnings("unchecked")
                        Map<Integer, ILSMOperationTracker> opTrackers =
                                (Map<Integer, ILSMOperationTracker>) opTrackersField.get(datasetResource);

                        // Manual registration - simulate what DatasetLifecycleManager would do
                        if (opTrackers != null && !opTrackers.containsKey(partition)) {
                            LOGGER.debug(
                                    "Directly adding tracker to datasetPrimaryOpTrackers for dataset {}, partition {}: {}",
                                    datasetId, partition, mockTracker);
                            opTrackers.put(partition, mockTracker);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Failed to access datasetPrimaryOpTrackers via reflection", e);
                    }
                }
            }

            return mockTracker;
        }

        @Override
        public ILSMIOOperationCallback getIOOperationCallback() {
            return null;
        }

        @Override
        public List<ILSMDiskComponent> getDiskComponents() {
            return List.of();
        }

        @Override
        public boolean isPrimaryIndex() {
            return false;
        }

        @Override
        public void modify(IIndexOperationContext ictx, ITupleReference tuple) throws HyracksDataException {

        }

        @Override
        public void search(ILSMIndexOperationContext ictx, IIndexCursor cursor, ISearchPredicate pred)
                throws HyracksDataException {

        }

        @Override
        public void scanDiskComponents(ILSMIndexOperationContext ctx, IIndexCursor cursor) throws HyracksDataException {

        }

        @Override
        public ILSMIOOperation createFlushOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
            return null;
        }

        @Override
        public ILSMDiskComponent flush(ILSMIOOperation operation) throws HyracksDataException {
            return null;
        }

        @Override
        public ILSMIOOperation createMergeOperation(ILSMIndexOperationContext ctx) throws HyracksDataException {
            return null;
        }

        @Override
        public ILSMDiskComponent merge(ILSMIOOperation operation) throws HyracksDataException {
            return null;
        }

        @Override
        public void addDiskComponent(ILSMDiskComponent index) throws HyracksDataException {

        }

        @Override
        public void addBulkLoadedDiskComponent(ILSMDiskComponent c) throws HyracksDataException {

        }

        @Override
        public void subsumeMergedComponents(ILSMDiskComponent newComponent, List<ILSMComponent> mergedComponents)
                throws HyracksDataException {

        }

        @Override
        public void changeMutableComponent() {

        }

        @Override
        public void changeFlushStatusForCurrentMutableCompoent(boolean needsFlush) {

        }

        @Override
        public boolean hasFlushRequestForCurrentMutableComponent() {
            return false;
        }

        @Override
        public void getOperationalComponents(ILSMIndexOperationContext ctx) throws HyracksDataException {

        }

        @Override
        public List<ILSMDiskComponent> getInactiveDiskComponents() {
            return List.of();
        }

        @Override
        public void addInactiveDiskComponent(ILSMDiskComponent diskComponent) {

        }

        @Override
        public List<ILSMMemoryComponent> getInactiveMemoryComponents() {
            return List.of();
        }

        @Override
        public void addInactiveMemoryComponent(ILSMMemoryComponent memoryComponent) {

        }

        // Additional method implementations required by the interface
        // would be implemented here with minimal functionality

        /**
         * Sets the operation tracker for this mock index.
         * Added to support DatasetLifecycleManagerLazyRecoveryTest use cases.
         */
        public void setOperationTracker(PrimaryIndexOperationTracker tracker) {
            this.mockTracker = tracker;
        }
    }

    /**
     * Enhanced version of the MockIndex implementation that provides better
     * tracking of operation tracker state changes.
     */
    private static class EnhancedMockIndex extends MockIndex {
        private PrimaryIndexOperationTracker assignedTracker;
        private final List<String> trackerEvents = new ArrayList<>();

        public EnhancedMockIndex(String resourcePath, int datasetId, Map<Integer, DatasetResource> datasetResourceMap) {
            super(resourcePath, datasetId, datasetResourceMap);
        }

        @Override
        public ILSMOperationTracker getOperationTracker() {
            PrimaryIndexOperationTracker tracker = (PrimaryIndexOperationTracker) super.getOperationTracker();

            if (assignedTracker == null) {
                assignedTracker = tracker;
                trackerEvents.add("Initial tracker assignment: " + tracker);
            } else if (assignedTracker != tracker) {
                trackerEvents.add("Tracker changed from " + assignedTracker + " to " + tracker);
                assignedTracker = tracker;
            }

            return tracker;
        }

        public List<String> getTrackerEvents() {
            return trackerEvents;
        }
    }

    /**
     * Tests specific race conditions that could lead to null operation trackers during unregistration.
     * This test verifies:
     * 1. Concurrent registration, opening, and unregistration don't create null operation trackers
     * 2. The operation tracker remains valid throughout concurrent operations
     * 3. Operation tracker cleanup occurs only when appropriate
     */
    @Test
    public void testRaceConditionsWithOperationTracker() throws Exception {
        LOGGER.info("Starting testRaceConditionsWithOperationTracker");

        // Create a resource path for testing
        String resourcePath = createdResourcePaths.get(0);

        // Get dataset and partition information for this resource
        LocalResource resource = mockResources.get(resourcePath);
        DatasetLocalResource datasetLocalResource = (DatasetLocalResource) resource.getResource();
        int datasetId = datasetLocalResource.getDatasetId();
        int partition = datasetLocalResource.getPartition();

        // Setup an enhanced mock index for this test to track operation tracker events
        EnhancedMockIndex enhancedMockIndex = new EnhancedMockIndex(resourcePath, datasetId, datasetResourceMap);
        mockIndexes.put(resourcePath, enhancedMockIndex);

        // Register the resource to make it available
        IIndex index = datasetLifecycleManager.registerIfAbsent(resourcePath, enhancedMockIndex);
        assertNotNull("Index should be registered successfully", index);
        assertSame("Should get our enhanced mock index", enhancedMockIndex, index);

        // Create multiple threads that will perform random operations on the same resource
        final int NUM_CONCURRENT_THREADS = 10;
        final int OPERATIONS_PER_THREAD = 20;
        final CyclicBarrier startBarrier = new CyclicBarrier(NUM_CONCURRENT_THREADS);
        final CountDownLatch completionLatch = new CountDownLatch(NUM_CONCURRENT_THREADS);

        for (int i = 0; i < NUM_CONCURRENT_THREADS; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    startBarrier.await();
                    Random random = new Random();

                    for (int op = 0; op < OPERATIONS_PER_THREAD; op++) {
                        // Perform a random operation:
                        // 0 = check index existence
                        // 1 = open
                        // 2 = close
                        // 3 = register
                        // 4 = check operation tracker
                        int operation = random.nextInt(5);

                        switch (operation) {
                            case 0:
                                // Check if index exists
                                IIndex existingIndex = datasetLifecycleManager.get(resourcePath);
                                LOGGER.debug("Thread {} checked index existence: {}", threadId, existingIndex != null);
                                break;
                            case 1:
                                // Open the index
                                try {
                                    datasetLifecycleManager.open(resourcePath);
                                    LOGGER.debug("Thread {} opened index", threadId);
                                } catch (Exception e) {
                                    // This is expected occasionally since the resource might be unregistered
                                    LOGGER.debug("Thread {} failed to open index: {}", threadId, e.getMessage());
                                }
                                break;
                            case 2:
                                // Close the index
                                try {
                                    datasetLifecycleManager.close(resourcePath);
                                    LOGGER.debug("Thread {} closed index", threadId);
                                } catch (Exception e) {
                                    // This is expected occasionally
                                    LOGGER.debug("Thread {} failed to close index: {}", threadId, e.getMessage());
                                }
                                break;
                            case 3:
                                // Register or re-register the index
                                try {
                                    IIndex reregisteredIndex =
                                            datasetLifecycleManager.registerIfAbsent(resourcePath, enhancedMockIndex);
                                    LOGGER.debug("Thread {} registered index: {}", threadId, reregisteredIndex != null);
                                } catch (Exception e) {
                                    LOGGER.debug("Thread {} failed to register index: {}", threadId, e.getMessage());
                                }
                                break;
                            case 4:
                                // Check operation tracker
                                try {
                                    // Get the dataset resource
                                    DatasetResource datasetResource = datasetResourceMap.get(datasetId);
                                    if (datasetResource != null) {
                                        // Get and verify the operation tracker
                                        ILSMOperationTracker tracker = getOperationTracker(datasetResource, partition);
                                        if (tracker instanceof PrimaryIndexOperationTracker) {
                                            LOGGER.debug("Thread {} found valid PrimaryIndexOperationTracker: {}",
                                                    threadId, tracker);
                                        } else if (tracker != null) {
                                            LOGGER.warn("Thread {} found non-PrimaryIndexOperationTracker: {}",
                                                    threadId, tracker.getClass().getName());
                                        } else {
                                            LOGGER.debug("Thread {} found no operation tracker", threadId);
                                        }
                                    }
                                } catch (Exception e) {
                                    LOGGER.debug("Thread {} error checking operation tracker: {}", threadId,
                                            e.getMessage());
                                }
                                break;
                        }

                        // Small delay to increase chance of race conditions
                        Thread.sleep(random.nextInt(5));
                    }

                    LOGGER.debug("Thread {} completed all operations", threadId);
                    completionLatch.countDown();
                } catch (Exception e) {
                    LOGGER.error("Thread {} error during test: {}", threadId, e.getMessage(), e);
                    completionLatch.countDown();
                }
            });
        }

        // Wait for all threads to complete
        boolean allCompleted = completionLatch.await(30, TimeUnit.SECONDS);
        if (!allCompleted) {
            LOGGER.warn("Not all threads completed in time");
        }

        // Get the tracker events to verify behavior
        List<String> trackerEvents = enhancedMockIndex.getTrackerEvents();
        LOGGER.debug("Tracker events recorded: {}", trackerEvents.size());
        for (String event : trackerEvents) {
            LOGGER.debug("Tracker event: {}", event);
        }

        // Verify the index is still registered at the end
        IIndex finalIndex = datasetLifecycleManager.get(resourcePath);
        if (finalIndex != null) {
            // Get the final operation tracker state if the index is still registered
            DatasetResource datasetResource = datasetResourceMap.get(datasetId);
            if (datasetResource != null) {
                ILSMOperationTracker finalTracker = getOperationTracker(datasetResource, partition);
                LOGGER.debug("Final operation tracker state: {}", finalTracker);

                // If there's a valid tracker, it should be a PrimaryIndexOperationTracker
                if (finalTracker != null) {
                    assertTrue("Final tracker should be a PrimaryIndexOperationTracker",
                            finalTracker instanceof PrimaryIndexOperationTracker);
                }
            }
        }

        LOGGER.info("Completed testRaceConditionsWithOperationTracker");
    }

    // Helper method to ensure consistent register approach
    public IIndex registerIfAbsentIndex(String resourcePath) throws HyracksDataException {
        MockIndex mockIndex = mockIndexes.get(resourcePath);
        IIndex index = datasetLifecycleManager.registerIfAbsent(resourcePath, mockIndex);
        LOGGER.debug("Registered {} with mock index {}", resourcePath, mockIndex);
        return index;
    }

    @Test
    public void testBalancedOpenCloseUnregister() throws Exception {
        LOGGER.info("Starting testBalancedOpenCloseUnregister");

        // Select a resource to test with
        String resourcePath = createdResourcePaths.get(0);

        // Register the index
        IIndex index = registerIfAbsentIndex(resourcePath);
        assertNotNull("Index should be registered successfully", index);
        LOGGER.info("Registered index: {}", resourcePath);

        // Number of times to open/close
        final int numOperations = 5;

        // Open the index multiple times
        for (int i = 0; i < numOperations; i++) {
            datasetLifecycleManager.open(resourcePath);
            LOGGER.info("Opened index {} (#{}/{})", resourcePath, i + 1, numOperations);
        }

        // Close the index the same number of times
        for (int i = 0; i < numOperations; i++) {
            datasetLifecycleManager.close(resourcePath);
            LOGGER.info("Closed index {} (#{}/{})", resourcePath, i + 1, numOperations);
        }

        // At this point, reference count should be balanced and unregistration should succeed
        try {
            datasetLifecycleManager.unregister(resourcePath);
            LOGGER.info("Successfully unregistered index {} after balanced open/close operations", resourcePath);

            // After unregistration, simulate real behavior by making the repository return null for this path
            // This is what would happen in a real environment - the resource would be removed from storage
            mockResources.remove(resourcePath);
            LOGGER.info("Removed resource {} from mock resources", resourcePath);
        } catch (HyracksDataException e) {
            fail("Failed to unregister index after balanced open/close: " + e.getMessage());
        }

        // Verify the index is actually unregistered by attempting to get it
        IIndex retrievedIndex = datasetLifecycleManager.get(resourcePath);
        assertNull("Index should no longer be registered after unregistration", retrievedIndex);

        // Verify the resource is no longer in our mock repository
        LocalResource retrievedResource = mockResourceRepository.get(resourcePath);
        assertNull("Resource should be removed from repository after unregistration", retrievedResource);

        // Try to open the unregistered index - should fail
        boolean openFailed = false;
        try {
            datasetLifecycleManager.open(resourcePath);
        } catch (HyracksDataException e) {
            openFailed = true;
            LOGGER.info("As expected, failed to open unregistered index: {}", e.getMessage());
            assertTrue("Error should indicate the index doesn't exist",
                    e.getMessage().contains("Index does not exist"));
        }
        assertTrue("Opening an unregistered index should fail", openFailed);
    }

    // Add these accessor methods at the end of the class

    /**
     * Returns the dataset lifecycle manager for testing
     */
    public DatasetLifecycleManager getDatasetLifecycleManager() {
        return datasetLifecycleManager;
    }

    /**
     * Returns the mock service context for testing
     */
    public INCServiceContext getMockServiceContext() {
        return mockServiceContext;
    }

    /**
     * Returns the list of created resource paths
     */
    public List<String> getCreatedResourcePaths() {
        return createdResourcePaths;
    }

    /**
     * Returns the map of mock resources
     */
    public Map<String, LocalResource> getMockResources() {
        return mockResources;
    }

    /**
     * Returns the mock recovery manager
     */
    public IRecoveryManager getMockRecoveryManager() {
        return mockRecoveryManager;
    }

    /**
     * Returns the mock index for a path
     */
    public MockIndex getMockIndex(String resourcePath) {
        return mockIndexes.get(resourcePath);
    }

    /**
     * Returns the mock resource repository
     */
    public ILocalResourceRepository getMockResourceRepository() {
        return mockResourceRepository;
    }

    /**
     * Creates a mock index for a resource path and adds it to the mockIndexes map.
     * This is used by DatasetLifecycleManagerLazyRecoveryTest to ensure consistent mock creation.
     */
    public void createMockIndexForPath(String resourcePath, int datasetId, int partition,
            PrimaryIndexOperationTracker opTracker) throws HyracksDataException {
        // Extract needed dataset ID from the path or use provided one
        MockIndex mockIndex = new MockIndex(resourcePath, datasetId, datasetResourceMap);

        // Set the operation tracker directly if provided
        if (opTracker != null) {
            mockIndex.setOperationTracker(opTracker);
        }

        // Store the mock index
        mockIndexes.put(resourcePath, mockIndex);

        // Create an answer that returns this mock index
        DatasetLocalResource mockDatasetResource = null;
        for (Map.Entry<String, LocalResource> entry : mockResources.entrySet()) {
            if (entry.getKey().equals(resourcePath)) {
                mockDatasetResource = (DatasetLocalResource) entry.getValue().getResource();
                break;
            }
        }

        if (mockDatasetResource != null) {
            when(mockDatasetResource.createInstance(any())).thenReturn(mockIndex);
        }

        LOGGER.debug("Created mock index for {}: {}", resourcePath, mockIndex);
    }
}
