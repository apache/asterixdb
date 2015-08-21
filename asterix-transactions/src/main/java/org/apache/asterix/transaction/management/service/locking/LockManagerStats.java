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

package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

final class LockManagerStats {
    private final int loggingPeriod;
    
    private final AtomicLong lCnt = new AtomicLong();
    private final AtomicLong ilCnt = new AtomicLong();
    private final AtomicLong tlCnt = new AtomicLong();
    private final AtomicLong itlCnt = new AtomicLong();
    private final AtomicLong ulCnt = new AtomicLong();
    private final AtomicLong rlCnt = new AtomicLong();

    LockManagerStats(int loggingPeriod) {
        this.loggingPeriod = loggingPeriod;
    }
    
    final void lock()           { lCnt.incrementAndGet(); }
    final void instantLock()    { ilCnt.incrementAndGet(); }
    final void tryLock()        { tlCnt.incrementAndGet(); }
    final void instantTryLock() { itlCnt.incrementAndGet(); }
    final void unlock()         { ulCnt.incrementAndGet(); }
    final void releaseLocks()   { rlCnt.incrementAndGet(); }
    
    final int requestSum() {
        return lCnt.intValue() + ilCnt.intValue() + tlCnt.intValue() 
                + itlCnt.intValue() + ulCnt.intValue() + rlCnt.intValue();
    }

    final StringBuilder append(StringBuilder sb) {
        sb.append("{")
        .append(" lock : ").append(lCnt)
        .append(", instantLock : ").append(ilCnt)
        .append(", tryLock : ").append(tlCnt)
        .append(", instantTryLock : ").append(itlCnt)
        .append(", unlock : ").append(ulCnt)
        .append(", releaseLocks : ").append(rlCnt)
        .append(" }");
        return sb;
    }        

    @Override
    public String toString() {
        return append(new StringBuilder()).toString();
    }

    final void logCounters(final Logger logger, final Level lvl, boolean always) {
        if (logger.isLoggable(lvl) 
            && (always || requestSum()  % loggingPeriod == 0)) {
            logger.log(lvl, toString());
        }
    }
}
