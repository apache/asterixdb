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
package org.apache.hyracks.control.common.config;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.IOptionType;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.util.Span;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.kohsuke.args4j.CmdLineException;

public class ConfigManagerTest {

    public enum Option implements IOption {
        OPTION1,
        OPTION2,
        OPTION3,
        OPTION4,
        OPTION5;

        @Override
        public Section section() {
            return Section.values()[this.ordinal() % Section.values().length];
        }

        @Override
        public String description() {
            return "Description for " + name();
        }

        @Override
        public IOptionType type() {
            return OptionTypes.INTEGER;
        }

        @Override
        public Object defaultValue() {
            return name() + " default value";
        }
    }

    public enum AliasedOption implements IOption {
        RENAMED_OPTION(List.of("RENAMEDOPTION"));

        private final List<String> aliases;

        AliasedOption(List<String> aliases) {
            this.aliases = aliases;
        }

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return "Description for " + name();
        }

        @Override
        public IOptionType type() {
            return OptionTypes.INTEGER;
        }

        @Override
        public Object defaultValue() {
            return 0;
        }

        @Override
        public List<String> aliases() {
            return aliases;
        }
    }

    public enum CollidingOption implements IOption {
        RENAMEDOPTION;

        @Override
        public Section section() {
            return Section.COMMON;
        }

        @Override
        public String description() {
            return "Description for " + name();
        }

        @Override
        public IOptionType type() {
            return OptionTypes.INTEGER;
        }

        @Override
        public Object defaultValue() {
            return 0;
        }
    }

    private static final Random RANDOM = new Random();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testConcurrentUpdates() throws Exception {
        ConfigManager configManager = new ConfigManager();
        configManager.register(Option.class);
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<Void>> futures = new ArrayList<>();
        IntStream.range(0, 20).forEach(a -> futures.add(executor.submit(() -> {
            Span.start(30, TimeUnit.SECONDS).loopUntilExhausted(() -> {
                String node = "node" + RANDOM.nextInt(5);
                IntStream.range(0, 20).parallel().forEach(a1 -> {
                    if (RANDOM.nextBoolean()) {
                        configManager.set(node, randomOption(), RANDOM.nextInt());
                    } else {
                        configManager.getNodeEffectiveConfig(node).get(randomOption());
                    }
                    if (RANDOM.nextBoolean()) {
                        configManager.forgetNode(node);
                    }
                });
            });
            return null;
        })));
        MutableObject<Exception> failure = new MutableObject<>();
        futures.forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                if (failure.getValue() == null) {
                    failure.setValue(e);
                } else {
                    failure.getValue().addSuppressed(e);
                }
            }
        });
        if (failure.getValue() != null) {
            throw failure.getValue();
        }
    }

    @Test
    public void testIniAlias() throws Exception {
        ConfigManager configManager = aliasedConfigManager(iniArgs("renamedoption = 7"));
        configManager.processConfig();
        Assert.assertEquals(7, configManager.get(AliasedOption.RENAMED_OPTION));
    }

    @Test
    public void testIniAliasWithCanonicalNameRejected() throws Exception {
        ConfigManager configManager = aliasedConfigManager(iniArgs("renamedoption = 7", "renamed.option = 8"));
        HyracksException e = Assert.assertThrows(HyracksException.class, configManager::processConfig);
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("renamed.option"));
    }

    @Test
    public void testCommandLineAlias() throws Exception {
        ConfigManager configManager = aliasedConfigManager(new String[] { "-renamedoption", "5" });
        configManager.processConfig();
        Assert.assertEquals(5, configManager.get(AliasedOption.RENAMED_OPTION));
    }

    @Test
    public void testCommandLineAliasWithCanonicalNameRejected() {
        ConfigManager configManager =
                aliasedConfigManager(new String[] { "-renamedoption", "5", "-renamed-option", "6" });
        Assert.assertThrows(CmdLineException.class, configManager::processConfig);
    }

    @Test
    public void testLookupByAlias() {
        ConfigManager configManager = aliasedConfigManager(null);
        Assert.assertEquals(AliasedOption.RENAMED_OPTION, configManager.lookupOption("common", "renamedoption"));
        Assert.assertEquals(AliasedOption.RENAMED_OPTION, configManager.lookupOption("common", "renamed.option"));
        Assert.assertNull(configManager.lookupOption("nc", "renamedoption"));
    }

    @Test
    public void testAliasCollidingWithOptionRejected() {
        ConfigManager aliasFirst = new ConfigManager();
        aliasFirst.register(AliasedOption.class);
        Assert.assertThrows(IllegalStateException.class, () -> aliasFirst.register(CollidingOption.class));
        ConfigManager optionFirst = new ConfigManager();
        optionFirst.register(CollidingOption.class);
        Assert.assertThrows(IllegalStateException.class, () -> optionFirst.register(AliasedOption.class));
    }

    private String[] iniArgs(String... lines) throws Exception {
        File ini = tempFolder.newFile("aliases.ini");
        Files.writeString(ini.toPath(), "[common]\n" + String.join("\n", lines) + "\n", StandardCharsets.UTF_8);
        return new String[] { "-config-file", ini.getAbsolutePath() };
    }

    private static ConfigManager aliasedConfigManager(String[] args) {
        ConfigManager configManager = new ConfigManager(args);
        configManager.addIniParamOptions(ControllerConfig.Option.CONFIG_FILE);
        configManager.addCmdLineSections(Section.COMMON);
        configManager.register(ControllerConfig.Option.class);
        configManager.register(AliasedOption.class);
        return configManager;
    }

    private static Option randomOption() {
        return Option.values()[RANDOM.nextInt(Option.values().length)];
    }
}
