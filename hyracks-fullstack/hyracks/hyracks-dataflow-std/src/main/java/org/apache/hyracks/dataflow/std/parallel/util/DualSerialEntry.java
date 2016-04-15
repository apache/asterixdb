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
package org.apache.hyracks.dataflow.std.parallel.util;

import java.util.Map.Entry;

public class DualSerialEntry<K extends Comparable<? super K>, V extends Comparable<? super V>> implements Entry<K, V>,
        Comparable<DualSerialEntry<K, V>> {
    private final K key;
    private V value;
    private boolean obf = true;
    private boolean desc = false;

    public DualSerialEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public DualSerialEntry(K key, V value, boolean obf) {
        this(key, value);
        this.obf = obf;
    }

    public DualSerialEntry(K key, V value, boolean obf, boolean desc) {
        this(key, value, obf);
        this.desc = desc;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        return this.value = value;
    }

    @Override
    public int compareTo(DualSerialEntry<K, V> o) {
        if (obf) {
            if (desc)
                return (o.getValue().compareTo(value));
            else
                return (value.compareTo(o.getValue()));
        } else {
            if (desc)
                return (o.getKey().compareTo(key));
            else
                return (key.compareTo(o.getKey()));
        }
    }
}
