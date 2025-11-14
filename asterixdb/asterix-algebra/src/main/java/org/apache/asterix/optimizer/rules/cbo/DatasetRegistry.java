
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
package org.apache.asterix.optimizer.rules.cbo;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DatasetRegistry {
    private final Map<AbstractLeafInput, Long> datasetMap;
    private final List<AbstractLeafInput> datasets;

    public DatasetRegistry() {
        datasets = new java.util.ArrayList<>();
        datasetMap = new java.util.HashMap<>();
    }

    public void addDataset(AbstractLeafInput dataset) {
        // Duplicate Datasets?
        datasets.add(dataset);
        datasetMap.put(dataset, (long) (datasets.size() - 1));
    }

    public List<AbstractLeafInput> getDatasets() {
        return datasets;
    }

    public class DatasetSubset {
        private Long bitset;

        public DatasetSubset(AbstractLeafInput dataset) {
            this.bitset = (1L << datasetMap.get(dataset));
        }

        public DatasetSubset() {
            bitset = 0L;
        }

        public void addDataset(AbstractLeafInput dataset) {
            bitset |= (1L << datasetMap.get(dataset));
        }

        public Long getBitset() {
            return bitset;
        }

        // Always use DatasetRegistry.equals to compare DatasetSubsets and not ==
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;
            DatasetSubset other = (DatasetSubset) obj;
            return Objects.equals(bitset, other.bitset) && DatasetRegistry.this == other.getEnclosingRegistry();
        }

        private DatasetRegistry getEnclosingRegistry() {
            return DatasetRegistry.this;
        }

        public int size() {
            int count = 0;
            for (int i = 0; i < datasets.size(); i++) {
                if ((bitset & (1L << i)) != 0) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < datasets.size(); i++) {
                if ((bitset & (1L << i)) != 0) {
                    sb.append(datasets.get(i).toString());
                }
            }
            return sb.toString();
        }

    }

    public DatasetSubset union(DatasetSubset ds1, DatasetSubset ds2) {
        DatasetSubset result = new DatasetSubset();
        result.bitset = ds1.bitset | ds2.bitset;
        return result;
    }

    public DatasetSubset intersection(DatasetSubset ds1, DatasetSubset ds2) {
        DatasetSubset result = new DatasetSubset();
        result.bitset = ds1.bitset & ds2.bitset;
        return result;
    }

    public static boolean isSubset(DatasetSubset one, DatasetSubset two) {
        return (one.bitset & two.bitset) == one.bitset;
    }

    public static boolean equals(DatasetSubset one, DatasetSubset two) {
        return Objects.equals(one.bitset, two.bitset);
    }

}
