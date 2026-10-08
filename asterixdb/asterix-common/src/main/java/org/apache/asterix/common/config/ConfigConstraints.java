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
package org.apache.asterix.common.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

/**
 * Constraints that span more than one option, which a single option's type cannot express. Each mirrors the
 * arithmetic of the component that consumes the options, so that a combination which would leave that component
 * with no pages or frames, or overflow its int arithmetic, is reported before it is used.
 */
public final class ConfigConstraints {

    public static final class Violation {
        private final String message;
        private final boolean fatalAtStartup;

        private Violation(String message, boolean fatalAtStartup) {
            this.message = message;
            this.fatalAtStartup = fatalAtStartup;
        }

        public String getMessage() {
            return message;
        }

        /**
         * @return true when a node started with this configuration cannot come up; false when the violation only
         *         fails the work that uses the options (e.g. every query)
         */
        public boolean isFatalAtStartup() {
            return fatalAtStartup;
        }

        @Override
        public String toString() {
            return message;
        }
    }

    private static final class CompilerMemory {
        private final IOption option;
        private final int minFrames;

        private CompilerMemory(IOption option, int minFrames) {
            this.option = option;
            this.minFrames = minFrames;
        }
    }

    private static final List<CompilerMemory> COMPILER_MEMORY = List.of(
            new CompilerMemory(CompilerProperties.Option.COMPILER_SORTMEMORY,
                    OptimizationConfUtil.MIN_FRAME_LIMIT_FOR_SORT),
            new CompilerMemory(CompilerProperties.Option.COMPILER_JOINMEMORY,
                    OptimizationConfUtil.MIN_FRAME_LIMIT_FOR_JOIN),
            new CompilerMemory(CompilerProperties.Option.COMPILER_GROUPMEMORY,
                    OptimizationConfUtil.MIN_FRAME_LIMIT_FOR_GROUP_BY),
            new CompilerMemory(CompilerProperties.Option.COMPILER_WINDOWMEMORY,
                    OptimizationConfUtil.MIN_FRAME_LIMIT_FOR_WINDOW),
            new CompilerMemory(CompilerProperties.Option.COMPILER_TEXTSEARCHMEMORY,
                    OptimizationConfUtil.MIN_FRAME_LIMIT_FOR_TEXT_SEARCH),
            new CompilerMemory(CompilerProperties.Option.COMPILER_CLUSTERBYMEMORY,
                    OptimizationConfUtil.MIN_FRAME_LIMIT_FOR_CLUSTER_BY));

    private ConfigConstraints() {
    }

    /**
     * @param config the value each option takes (or would take) in the configuration being checked
     * @return every violated constraint; empty when the configuration is consistent
     */
    public static List<Violation> check(Function<IOption, Object> config) {
        List<Violation> violations = new ArrayList<>();
        checkBufferCache(config, violations);
        checkMemoryComponents(config, violations);
        checkHeap(config, violations, StorageProperties.MAX_HEAP_BYTES);
        checkTransactionLog(config, violations);
        checkMessaging(config, violations);
        checkCompilerMemory(config, violations);
        checkActiveMemory(config, violations);
        return violations.isEmpty() ? Collections.emptyList() : violations;
    }

    /**
     * Throws when {@code proposed} violates a constraint that {@code current} does not.
     *
     * @throws IllegalArgumentException naming every violated constraint the change introduces
     */
    public static void validateChange(Function<IOption, Object> current, Function<IOption, Object> proposed) {
        Set<String> existing = new HashSet<>();
        for (Violation violation : check(current)) {
            existing.add(violation.getMessage());
        }
        List<Violation> introduced = new ArrayList<>();
        for (Violation violation : check(proposed)) {
            if (!existing.contains(violation.getMessage())) {
                introduced.add(violation);
            }
        }
        if (!introduced.isEmpty()) {
            throw new IllegalArgumentException("Invalid configuration: " + describe(introduced));
        }
    }

    public static String describe(List<Violation> violations) {
        StringBuilder sb = new StringBuilder();
        for (Violation v : violations) {
            if (sb.length() > 0) {
                sb.append("; ");
            }
            sb.append(v.getMessage());
        }
        return sb.toString();
    }

    private static void checkBufferCache(Function<IOption, Object> config, List<Violation> violations) {
        Long size = longValue(config, StorageProperties.Option.STORAGE_BUFFER_CACHE_SIZE);
        Long pageSize = longValue(config, StorageProperties.Option.STORAGE_BUFFER_CACHE_PAGE_SIZE);
        if (size == null || pageSize == null || pageSize <= 0) {
            return;
        }
        // StorageProperties.getBufferCacheNumPages
        checkPageCount(violations, StorageProperties.Option.STORAGE_BUFFER_CACHE_SIZE, size,
                StorageProperties.Option.STORAGE_BUFFER_CACHE_PAGE_SIZE, pageSize + IBufferCache.RESERVED_HEADER_BYTES,
                true);
    }

    private static void checkMemoryComponents(Function<IOption, Object> config, List<Violation> violations) {
        Long budget = longValue(config, StorageProperties.Option.STORAGE_MEMORY_COMPONENT_GLOBAL_BUDGET);
        Long pageSize = longValue(config, StorageProperties.Option.STORAGE_MEMORY_COMPONENT_PAGE_SIZE);
        if (budget == null || pageSize == null || pageSize <= 0) {
            return;
        }
        // GlobalVirtualBufferCache: VirtualBufferCache rejects a page budget of 0
        checkPageCount(violations, StorageProperties.Option.STORAGE_MEMORY_COMPONENT_GLOBAL_BUDGET, budget,
                StorageProperties.Option.STORAGE_MEMORY_COMPONENT_PAGE_SIZE, pageSize, true);
    }

    static void checkHeap(Function<IOption, Object> config, List<Violation> violations, long maxHeapBytes) {
        Long cacheSize = longValue(config, StorageProperties.Option.STORAGE_BUFFER_CACHE_SIZE);
        Long budget = longValue(config, StorageProperties.Option.STORAGE_MEMORY_COMPONENT_GLOBAL_BUDGET);
        if (cacheSize == null || budget == null) {
            return;
        }
        // StorageProperties.getJobExecutionMemoryBudget fails node startup when nothing is left for jobs
        if (cacheSize + budget >= maxHeapBytes) {
            violations.add(new Violation(String.format("%s (%d) plus %s (%d) must be less than the maximum heap (%d)",
                    StorageProperties.Option.STORAGE_BUFFER_CACHE_SIZE.ini(), cacheSize,
                    StorageProperties.Option.STORAGE_MEMORY_COMPONENT_GLOBAL_BUDGET.ini(), budget, maxHeapBytes),
                    true));
        }
    }

    private static void checkTransactionLog(Function<IOption, Object> config, List<Violation> violations) {
        Long pageSize = longValue(config, TransactionProperties.Option.TXN_LOG_BUFFER_PAGESIZE);
        Long numPages = longValue(config, TransactionProperties.Option.TXN_LOG_BUFFER_NUMPAGES);
        Long partitionSize = longValue(config, TransactionProperties.Option.TXN_LOG_PARTITIONSIZE);
        if (pageSize == null || numPages == null || partitionSize == null) {
            return;
        }
        // LogManagerProperties: the log buffer size is an int, and the partition size is rounded down to a
        // multiple of it, then divided by
        long bufferSize = pageSize * numPages;
        if (bufferSize > Integer.MAX_VALUE) {
            violations.add(new Violation(
                    String.format("%s (%d) times %s (%d) must not exceed %d bytes",
                            TransactionProperties.Option.TXN_LOG_BUFFER_PAGESIZE.ini(), pageSize,
                            TransactionProperties.Option.TXN_LOG_BUFFER_NUMPAGES.ini(), numPages, Integer.MAX_VALUE),
                    true));
        } else if (partitionSize < bufferSize) {
            violations.add(new Violation(String.format("%s (%d) must be at least %s times %s (%d)",
                    TransactionProperties.Option.TXN_LOG_PARTITIONSIZE.ini(), partitionSize,
                    TransactionProperties.Option.TXN_LOG_BUFFER_PAGESIZE.ini(),
                    TransactionProperties.Option.TXN_LOG_BUFFER_NUMPAGES.ini(), bufferSize), true));
        }
    }

    private static void checkMessaging(Function<IOption, Object> config, List<Violation> violations) {
        Long frameSize = longValue(config, MessagingProperties.Option.MESSAGING_FRAME_SIZE);
        Long frameCount = longValue(config, MessagingProperties.Option.MESSAGING_FRAME_COUNT);
        if (frameSize == null || frameCount == null) {
            return;
        }
        // NCMessageBroker multiplies these as ints to size its frame pool
        if (frameSize * frameCount > Integer.MAX_VALUE) {
            violations.add(new Violation(
                    String.format("%s (%d) times %s (%d) must not exceed %d bytes",
                            MessagingProperties.Option.MESSAGING_FRAME_SIZE.ini(), frameSize,
                            MessagingProperties.Option.MESSAGING_FRAME_COUNT.ini(), frameCount, Integer.MAX_VALUE),
                    true));
        }
    }

    private static void checkCompilerMemory(Function<IOption, Object> config, List<Violation> violations) {
        Long frameSize = longValue(config, CompilerProperties.Option.COMPILER_FRAMESIZE);
        if (frameSize == null || frameSize <= 0) {
            return;
        }
        // OptimizationConfUtil.getFrameLimit computes every one of these for every query, so one budget too small
        // for its operator fails all queries, not only those using the operator
        for (CompilerMemory memory : COMPILER_MEMORY) {
            Long budget = longValue(config, memory.option);
            if (budget == null) {
                continue;
            }
            long frames = budget / frameSize;
            if (frames < memory.minFrames) {
                violations.add(new Violation(
                        String.format("%s (%d) must hold at least %d frames of %s (%d)", memory.option.ini(), budget,
                                memory.minFrames, CompilerProperties.Option.COMPILER_FRAMESIZE.ini(), frameSize),
                        false));
            } else if (frames > Integer.MAX_VALUE) {
                violations.add(new Violation(
                        String.format("%s (%d) must not exceed %d frames of %s (%d)", memory.option.ini(), budget,
                                Integer.MAX_VALUE, CompilerProperties.Option.COMPILER_FRAMESIZE.ini(), frameSize),
                        false));
            }
        }
    }

    private static void checkActiveMemory(Function<IOption, Object> config, List<Violation> violations) {
        Long budget = longValue(config, ActiveProperties.Option.ACTIVE_MEMORY_GLOBAL_BUDGET);
        Long frameSize = longValue(config, CompilerProperties.Option.COMPILER_FRAMESIZE);
        if (budget == null || frameSize == null || frameSize <= 0) {
            return;
        }
        // ActiveManager's frame pool: with no whole frame in the budget, every feed runtime waits for a frame
        checkPageCount(violations, ActiveProperties.Option.ACTIVE_MEMORY_GLOBAL_BUDGET, budget,
                CompilerProperties.Option.COMPILER_FRAMESIZE, frameSize, false);
    }

    private static void checkPageCount(List<Violation> violations, IOption budgetOption, long budget,
            IOption pageSizeOption, long pageSize, boolean fatalAtStartup) {
        long pages = budget / pageSize;
        if (pages < 1) {
            violations.add(new Violation(String.format("%s (%d) must hold at least one %s (%d) page",
                    budgetOption.ini(), budget, pageSizeOption.ini(), pageSize), fatalAtStartup));
        } else if (pages > Integer.MAX_VALUE) {
            violations.add(new Violation(String.format("%s (%d) must not exceed %d pages of %s (%d)",
                    budgetOption.ini(), budget, Integer.MAX_VALUE, pageSizeOption.ini(), pageSize), fatalAtStartup));
        }
    }

    private static Long longValue(Function<IOption, Object> config, IOption option) {
        Object value = config.apply(option);
        return value instanceof Number ? ((Number) value).longValue() : null;
    }
}
