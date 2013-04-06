/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.transaction.management.service.transaction.MutableResourceId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;

public class IndexLoggerRepository {

    private final Map<MutableResourceId, IndexLogger> loggers = new HashMap<MutableResourceId, IndexLogger>();
    private final TransactionSubsystem txnSubsystem;
    private MutableResourceId mutableResourceId;

    public IndexLoggerRepository(TransactionSubsystem provider) {
        this.txnSubsystem = provider;
        mutableResourceId = new MutableResourceId(0);
    }

    public synchronized IndexLogger getIndexLogger(long resourceId, byte resourceType) {
        mutableResourceId.setId(resourceId);
        IndexLogger logger = loggers.get(mutableResourceId);
        if (logger == null) {
            MutableResourceId newMutableResourceId = new MutableResourceId(resourceId);
            IIndex index = (IIndex) txnSubsystem.getAsterixAppRuntimeContextProvider().getIndexLifecycleManager()
                    .getIndex(resourceId);
            logger = new IndexLogger(resourceId, resourceType, index);
            loggers.put(newMutableResourceId, logger);
        }
        return logger;
    }
}