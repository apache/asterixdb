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
package org.apache.hyracks.util.fastutil;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class Collectors {

    private static final Set<Characteristics> IDENTITY_CHARACTERISTICS =
            Collections.singleton(Characteristics.IDENTITY_FINISH);

    private Collectors() {
        throw new AssertionError("do not instantiate");
    }

    public static <T> Collector<Int2ObjectMap.Entry<T>, Int2ObjectMap<T>, Int2ObjectMap<T>> toInt2ObjectMap() {
        return new Collector<Int2ObjectMap.Entry<T>, Int2ObjectMap<T>, Int2ObjectMap<T>>() {
            @Override
            public Supplier<Int2ObjectMap<T>> supplier() {
                return Int2ObjectOpenHashMap::new;
            }

            @Override
            public BiConsumer<Int2ObjectMap<T>, Int2ObjectMap.Entry<T>> accumulator() {
                return (map, element) -> {
                    int key = element.getIntKey();
                    T value = Objects.requireNonNull(element.getValue());
                    T oldValue = map.putIfAbsent(key, value);
                    if (oldValue != null)
                        throw duplicateKey(key, oldValue, value);
                };
            }

            @Override
            public BinaryOperator<Int2ObjectMap<T>> combiner() {
                return (map1, map2) -> {
                    for (Int2ObjectMap.Entry<T> e : map2.int2ObjectEntrySet()) {
                        accumulator().accept(map1, e);
                    }
                    return map1;
                };
            }

            @Override
            public Function<Int2ObjectMap<T>, Int2ObjectMap<T>> finisher() {
                return Function.identity();
            }

            @Override
            public Set<Characteristics> characteristics() {
                return IDENTITY_CHARACTERISTICS;
            }

        };
    }

    public static <T> Collector<Long2ObjectMap.Entry<T>, Long2ObjectMap<T>, Long2ObjectMap<T>> toLong2ObjectMap() {
        return new Collector<Long2ObjectMap.Entry<T>, Long2ObjectMap<T>, Long2ObjectMap<T>>() {
            @Override
            public Supplier<Long2ObjectMap<T>> supplier() {
                return Long2ObjectOpenHashMap::new;
            }

            @Override
            public BiConsumer<Long2ObjectMap<T>, Long2ObjectMap.Entry<T>> accumulator() {
                return (map, element) -> {
                    long key = element.getLongKey();
                    T value = Objects.requireNonNull(element.getValue());
                    T oldValue = map.putIfAbsent(key, value);
                    if (oldValue != null)
                        throw duplicateKey(key, oldValue, value);
                };
            }

            @Override
            public BinaryOperator<Long2ObjectMap<T>> combiner() {
                return (map1, map2) -> {
                    for (Long2ObjectMap.Entry<T> e : map2.long2ObjectEntrySet()) {
                        accumulator().accept(map1, e);
                    }
                    return map1;
                };
            }

            @Override
            public Function<Long2ObjectMap<T>, Long2ObjectMap<T>> finisher() {
                return Function.identity();
            }

            @Override
            public Set<Characteristics> characteristics() {
                return IDENTITY_CHARACTERISTICS;
            }

        };
    }

    private static IllegalStateException duplicateKey(Object key, Object oldValue, Object newValue) {
        return new IllegalStateException("Duplicate key " + key + " (old: " + oldValue + ", new: " + newValue + ")");
    }

}
