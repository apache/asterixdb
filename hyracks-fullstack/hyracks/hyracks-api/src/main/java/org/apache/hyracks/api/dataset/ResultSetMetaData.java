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
package org.apache.hyracks.api.dataset;

import java.util.Arrays;

public class ResultSetMetaData {
    private final DatasetDirectoryRecord[] records;
    private final boolean ordered;

    ResultSetMetaData(int len, boolean ordered) {
        this.records = new DatasetDirectoryRecord[len];
        this.ordered = ordered;
    }

    public boolean getOrderedResult() {
        return ordered;
    }

    public DatasetDirectoryRecord[] getRecords() {
        return records;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ordered: ").append(ordered).append(", records: ").append(Arrays.toString(records));
        return sb.toString();
    }
}
