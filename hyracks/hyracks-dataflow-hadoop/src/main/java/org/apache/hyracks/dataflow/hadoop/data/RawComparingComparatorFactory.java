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
package org.apache.hyracks.dataflow.hadoop.data;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hyracks.api.dataflow.value.IComparator;
import org.apache.hyracks.api.dataflow.value.IComparatorFactory;
import org.apache.hyracks.dataflow.common.util.ReflectionUtils;

public class RawComparingComparatorFactory<T> implements IComparatorFactory<WritableComparable<T>> {
    private Class<? extends RawComparator> klass;

    public RawComparingComparatorFactory(Class<? extends RawComparator> klass) {
        this.klass = klass;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public IComparator<WritableComparable<T>> createComparator() {
        final RawComparator instance = ReflectionUtils.createInstance(klass);
        return new IComparator<WritableComparable<T>>() {
            @Override
            public int compare(WritableComparable<T> o1, WritableComparable<T> o2) {
                return instance.compare(o1, o2);
            }
        };
    }
}