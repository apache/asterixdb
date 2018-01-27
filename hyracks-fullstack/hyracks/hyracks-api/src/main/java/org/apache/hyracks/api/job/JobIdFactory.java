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
package org.apache.hyracks.api.job;

import static org.apache.hyracks.api.job.JobId.ID_BITS;
import static org.apache.hyracks.api.job.JobId.MAX_ID;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hyracks.api.control.CcId;

public class JobIdFactory {
    private final AtomicLong id;

    public JobIdFactory(CcId ccId) {
        id = new AtomicLong((long) ccId.shortValue() << ID_BITS);
    }

    public JobId create() {
        return new JobId(id.getAndUpdate(prev -> {
            if ((prev & MAX_ID) == MAX_ID) {
                return prev ^ MAX_ID;
            } else {
                return prev + 1;
            }
        }));
    }

    public JobId maxJobId() {
        long next = id.get();
        if ((next & MAX_ID) == 0) {
            return new JobId(next | MAX_ID);
        } else {
            return new JobId(next - 1);
        }
    }
}
