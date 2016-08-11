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
package org.apache.hyracks.api.dataflow.value;

public interface IRangePartitionType {
    public enum RangePartitioningType {
        /**
         * Partitioning is determined by finding the range partition where the first data point lies.
         */
        PROJECT,
        /**
         * Partitioning is determined by finding the range partition where the last data point lies.
         */
        PROJECT_END,
        /**
         * Partitioning is determined by finding all the range partitions where the data has a point.
         */
        SPLIT,
        /**
         * Partitioning is determined by finding all the range partitions where the data has a point
         * or comes after the data point.
         */
        REPLICATE
    }
}
