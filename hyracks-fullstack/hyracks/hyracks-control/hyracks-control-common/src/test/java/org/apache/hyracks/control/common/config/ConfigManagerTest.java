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
import org.apache.hyracks.util.Span;
import org.junit.Test;

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

    private static final Random RANDOM = new Random();

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

    private static Option randomOption() {
        return Option.values()[RANDOM.nextInt(Option.values().length)];
    }
}
