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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILogger;
import edu.uci.ics.asterix.common.transactions.ILoggerRepository;
import edu.uci.ics.asterix.common.transactions.ITransactionSubsystem;
import edu.uci.ics.asterix.common.transactions.MutableResourceId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;

public class IndexLoggerRepository implements ILoggerRepository {

    private final Map<MutableResourceId, ILogger> loggers = new HashMap<MutableResourceId, ILogger>();
    private final ITransactionSubsystem txnSubsystem;
    private MutableResourceId mutableResourceId;

    public IndexLoggerRepository(ITransactionSubsystem provider) {
        this.txnSubsystem = provider;
        mutableResourceId = new MutableResourceId(0);
    }

    @Override
    public synchronized ILogger getIndexLogger(long resourceId, byte resourceType) throws ACIDException {
        mutableResourceId.setId(resourceId);
        ILogger logger = loggers.get(mutableResourceId);
        if (logger == null) {
            MutableResourceId newMutableResourceId = new MutableResourceId(resourceId);
            IIndex index;
            try {
                index = (IIndex) txnSubsystem.getAsterixAppRuntimeContext().getIndexLifecycleManager()
                        .getIndex(resourceId);
            } catch (HyracksDataException e) {
                throw new ACIDException(e);
            }
            logger = new IndexLogger(resourceId, resourceType, index);
            loggers.put(newMutableResourceId, logger);
        }
        return logger;
    }
}