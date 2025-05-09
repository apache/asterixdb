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
package org.apache.asterix.optimizer.rules.cbo.indexadvisor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections4.iterators.PermutationIterator;

public interface IIndexEnumerator {

    Iterator<List<List<String>>> getIterator();

    void init(Collection<List<String>> fieldNames);

    static IIndexEnumerator allFieldsEnumerator() {

        return new IIndexEnumerator() {
            private PermutationIterator<List<String>> permutationIterator;

            @Override
            public Iterator<List<List<String>>> getIterator() {
                return permutationIterator;
            }

            @Override
            public void init(Collection<List<String>> fieldNames) {
                this.permutationIterator = new PermutationIterator<>(new ArrayList<>(fieldNames));
            }
        };
    }

    static IIndexEnumerator alphabeticEnumerator() {
        return new IIndexEnumerator() {
            private List<List<String>> listOfAllFields;

            @Override
            public Iterator<List<List<String>>> getIterator() {
                ArrayList<List<List<String>>> list = new ArrayList<>();
                list.add(listOfAllFields);
                return list.iterator();
            }

            @Override
            public void init(Collection<List<String>> fieldNames) {
                this.listOfAllFields = new ArrayList<>(fieldNames);
                listOfAllFields.sort((list1, list2) -> {
                    int minSize = Math.min(list1.size(), list2.size());
                    for (int i = 0; i < minSize; i++) {
                        int comparison = list1.get(i).compareTo(list2.get(i));
                        if (comparison != 0) {
                            return comparison;
                        }
                    }
                    return Integer.compare(list1.size(), list2.size());
                });

            }
        };
    }

}
