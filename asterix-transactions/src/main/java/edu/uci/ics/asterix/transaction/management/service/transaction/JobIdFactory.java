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
package edu.uci.ics.asterix.transaction.management.service.transaction;

import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.asterix.common.transactions.JobId;

/**
 * Represents a factory to generate unique transaction IDs.
 */
public class JobIdFactory {
    private static final AtomicInteger Id = new AtomicInteger();

    public static JobId generateJobId() {
        return new JobId(Id.incrementAndGet());
    }
    
    public static void initJobId(int id) {
        Id.set(id);
    }
}