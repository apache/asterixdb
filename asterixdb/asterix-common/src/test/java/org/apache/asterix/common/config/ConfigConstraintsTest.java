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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IConfigValidator;
import org.apache.hyracks.api.config.IOption;
import org.junit.Before;
import org.junit.Test;

public class ConfigConstraintsTest {

    private static final long KB = 1024L;
    private static final long MB = 1024L * KB;

    private final Map<IOption, Object> config = new HashMap<>();

    @Before
    public void setUp() {
        config.put(StorageProperties.Option.STORAGE_BUFFERCACHE_SIZE, 32 * MB);
        config.put(StorageProperties.Option.STORAGE_BUFFERCACHE_PAGESIZE, (int) (128 * KB));
        config.put(StorageProperties.Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET, 32 * MB);
        config.put(StorageProperties.Option.STORAGE_MEMORYCOMPONENT_PAGESIZE, (int) (128 * KB));
        config.put(TransactionProperties.Option.TXN_LOG_BUFFER_PAGESIZE, (int) (4 * MB));
        config.put(TransactionProperties.Option.TXN_LOG_BUFFER_NUMPAGES, 8);
        config.put(TransactionProperties.Option.TXN_LOG_PARTITIONSIZE, 256 * MB);
        config.put(MessagingProperties.Option.MESSAGING_FRAME_SIZE, (int) (4 * KB));
        config.put(MessagingProperties.Option.MESSAGING_FRAME_COUNT, 512);
        config.put(CompilerProperties.Option.COMPILER_FRAMESIZE, (int) (32 * KB));
        config.put(CompilerProperties.Option.COMPILER_SORTMEMORY, 32 * MB);
        config.put(CompilerProperties.Option.COMPILER_JOINMEMORY, 32 * MB);
        config.put(CompilerProperties.Option.COMPILER_GROUPMEMORY, 32 * MB);
        config.put(CompilerProperties.Option.COMPILER_WINDOWMEMORY, 32 * MB);
        config.put(CompilerProperties.Option.COMPILER_TEXTSEARCHMEMORY, 32 * MB);
        config.put(CompilerProperties.Option.COMPILER_CLUSTERBYMEMORY, 32 * MB);
        config.put(ActiveProperties.Option.ACTIVE_MEMORY_GLOBAL_BUDGET, 64 * MB);
    }

    @Test
    public void consistentConfigurationPasses() {
        assertTrue(ConfigConstraints.check(config::get).isEmpty());
    }

    @Test
    public void memoryComponentBudgetBelowOnePageIsFatal() {
        config.put(StorageProperties.Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET, 64 * KB);
        List<ConfigConstraints.Violation> violations = ConfigConstraints.check(config::get);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).isFatalAtStartup());
        assertTrue(violations.get(0).getMessage(),
                violations.get(0).getMessage().contains("storage.memorycomponent.globalbudget"));
    }

    @Test
    public void bufferCacheBelowOnePageIsFatal() {
        config.put(StorageProperties.Option.STORAGE_BUFFERCACHE_SIZE, 128 * KB);
        List<ConfigConstraints.Violation> violations = ConfigConstraints.check(config::get);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).isFatalAtStartup());
    }

    @Test
    public void budgetsFillingTheHeapAreFatal() {
        List<ConfigConstraints.Violation> violations = new ArrayList<>();
        ConfigConstraints.checkHeap(config::get, violations, 64 * MB);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).isFatalAtStartup());
        violations.clear();
        ConfigConstraints.checkHeap(config::get, violations, 65 * MB);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void transactionLogPartitionSmallerThanBufferIsFatal() {
        config.put(TransactionProperties.Option.TXN_LOG_PARTITIONSIZE, 16 * MB);
        List<ConfigConstraints.Violation> violations = ConfigConstraints.check(config::get);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).isFatalAtStartup());
    }

    @Test
    public void transactionLogBufferOverflowIsFatal() {
        config.put(TransactionProperties.Option.TXN_LOG_BUFFER_NUMPAGES, 1024);
        List<ConfigConstraints.Violation> violations = ConfigConstraints.check(config::get);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).getMessage().contains("must not exceed"));
    }

    @Test
    public void messagingPoolOverflowIsFatal() {
        config.put(MessagingProperties.Option.MESSAGING_FRAME_SIZE, (int) (8 * MB));
        List<ConfigConstraints.Violation> violations = ConfigConstraints.check(config::get);
        assertEquals(1, violations.size());
        assertTrue(violations.get(0).isFatalAtStartup());
    }

    @Test
    public void compilerMemoryBelowMinimumFramesFailsOnlyQueries() {
        config.put(CompilerProperties.Option.COMPILER_JOINMEMORY, 64 * KB);
        List<ConfigConstraints.Violation> violations = ConfigConstraints.check(config::get);
        assertEquals(1, violations.size());
        assertFalse(violations.get(0).isFatalAtStartup());
        assertTrue(violations.get(0).getMessage().contains("compiler.joinmemory"));
    }

    @Test
    public void raisingFrameSizeAloneCanBreakCompilerMemory() {
        config.put(CompilerProperties.Option.COMPILER_FRAMESIZE, (int) (16 * MB));
        List<ConfigConstraints.Violation> violations = ConfigConstraints.check(config::get);
        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().noneMatch(ConfigConstraints.Violation::isFatalAtStartup));
    }

    @Test
    public void activeBudgetBelowOneFrameIsReported() {
        config.put(ActiveProperties.Option.ACTIVE_MEMORY_GLOBAL_BUDGET, 16 * KB);
        List<ConfigConstraints.Violation> violations = ConfigConstraints.check(config::get);
        assertEquals(1, violations.size());
        assertFalse(violations.get(0).isFatalAtStartup());
    }

    @Test
    public void unsetOptionsAreSkipped() {
        config.clear();
        assertTrue(ConfigConstraints.check(config::get).isEmpty());
    }

    @Test
    public void validatorRejectsViolationsTheChangeIntroduces() {
        Map<IOption, Object> proposed = new HashMap<>(config);
        proposed.put(StorageProperties.Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET, 0L);
        proposed.put(CompilerProperties.Option.COMPILER_SORTMEMORY, KB);
        IConfigValidator validator = (option, value) -> {
        };
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> validator.validateChange(config::get, proposed::get));
        assertTrue(e.getMessage(), e.getMessage().contains("storage.memorycomponent.globalbudget"));
        assertTrue(e.getMessage(), e.getMessage().contains("compiler.sortmemory"));
    }

    @Test
    public void validatorAllowsCorrectingOneOfSeveralViolations() {
        config.put(StorageProperties.Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET, 0L);
        config.put(CompilerProperties.Option.COMPILER_SORTMEMORY, KB);
        Map<IOption, Object> proposed = new HashMap<>(config);
        proposed.put(StorageProperties.Option.STORAGE_MEMORYCOMPONENT_GLOBALBUDGET, 32 * MB);
        IConfigValidator validator = (option, value) -> {
        };
        validator.validateChange(config::get, proposed::get);
        proposed.put(ActiveProperties.Option.ACTIVE_MEMORY_GLOBAL_BUDGET, KB);
        assertThrows(IllegalArgumentException.class, () -> validator.validateChange(config::get, proposed::get));
    }
}
