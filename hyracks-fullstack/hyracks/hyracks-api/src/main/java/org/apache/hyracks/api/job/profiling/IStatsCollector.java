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
package org.apache.hyracks.api.job.profiling;

import java.io.Serializable;
import java.util.Map;

import org.apache.hyracks.api.dataflow.IPassableTimer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IWritable;

public interface IStatsCollector extends IWritable, Serializable {

    /**
     * Adds {@link IOperatorStats} to the stats collections
     *
     * @param operatorStats
     * @throws HyracksDataException when an operator with the same was already added.
     */
    void add(IOperatorStats operatorStats) throws HyracksDataException;

    /**
     * @param operatorName
     * @return {@link IOperatorStats} for the operator with name <code>operatorName</code>
     * if it already exists, and adds it if it does not.
     */
    IOperatorStats getOrAddOperatorStats(String operatorName);

    /**
     * Get every registered operator stats object
     * @return All registered operators, and their collected stats, with the names as keys and stats as values
     */
    Map<String, IOperatorStats> getAllOperatorStats();

    /**
     * @return A special {@link IOperatorStats} that has the aggregated stats
     * from all operators in the collection.
     */
    IOperatorStats getAggregatedStats();

    /**
     * Pause an operator's timer, to pass it to another operator
     * @param newHolder the timer that is starting execution
     * @return the current nanoTime when the clock was taken from the other operator
     */
    long takeClock(IPassableTimer newHolder);

    /**
     * Resume an operator's timer, when a downstream operator has finished execution of
     * the method the upstream operator called
     * @param currHolder the timer that needs to be paused
     */
    void giveClock(IPassableTimer currHolder);

}
