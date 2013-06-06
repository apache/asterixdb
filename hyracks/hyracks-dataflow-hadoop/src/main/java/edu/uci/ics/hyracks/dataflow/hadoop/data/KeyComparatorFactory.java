/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.hadoop.data;

import org.apache.hadoop.io.RawComparator;

import edu.uci.ics.hyracks.api.dataflow.value.IComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.util.ReflectionUtils;

public class KeyComparatorFactory<T> implements IComparatorFactory<T> {
    private static final long serialVersionUID = 1L;
    private Class<? extends RawComparator<T>> cmpClass;

    public KeyComparatorFactory(Class<? extends RawComparator<T>> cmpClass) {
        this.cmpClass = cmpClass;
    }

    @Override
    public IComparator<T> createComparator() {
        final RawComparator<T> instance = ReflectionUtils.createInstance(cmpClass);
        return new IComparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                return instance.compare(o1, o2);
            }
        };
    }
}