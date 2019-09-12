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

package org.apache.hyracks.client.stats;

import org.apache.hyracks.api.com.job.profiling.counters.Counter;

public class AggregateCounter extends Counter {
    private static final long serialVersionUID = 9140555872026977436L;

    private long sum = 0;
    private long numOfItems = 0;

    public AggregateCounter(String name) {
        super(name);
    }

    @Override
    public long set(long value) {
        long retVal = getRetValue();
        sum += value;
        numOfItems++;
        return retVal;
    }

    @Override
    public long get() {
        long retVal = getRetValue();
        return retVal;
    }

    public void reset() {
        sum = 0;
        numOfItems = 0;
    }

    private long getRetValue() {
        long retVal = 0;
        if (numOfItems != 0) {
            retVal = sum / numOfItems;
        } else {
            retVal = 0;
        }
        return retVal;
    }

}
