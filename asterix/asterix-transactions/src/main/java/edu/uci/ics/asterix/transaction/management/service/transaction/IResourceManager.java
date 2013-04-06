/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;

/**
 * Provides APIs for undo or redo of an operation on a resource.
 */
public interface IResourceManager {
    
    public class ResourceType {
        public static final byte LSM_BTREE = 1;
        public static final byte LSM_RTREE = 2;
        public static final byte LSM_INVERTED_INDEX = 3;
    }

    /**
     * Returns the unique identifier for the resource manager.
     * 
     * @return a unique identifier for the resource manager. The number of
     *         resource managers in the system are expected to be handful and
     *         can be uniquely identified by using a single byte as an id.
     */
    public byte getResourceManagerId();

    /**
     * Undo the operation corresponding to a given log record.
     * 
     * @param logRecordHelper
     *            (@see ILogRecordHelper) An implementation of the
     *            ILogRecordHelper interface that is used to parse the log
     *            record and extract useful information from the content.
     * @param LogicalLogLocator
     *            (@see LogicalLogLocator) The locationof the log record that
     *            needs to be undone.
     * @throws ACIDException
     */
    public void undo(ILogRecordHelper logRecordHelper, LogicalLogLocator logicalLogLocator) throws ACIDException;

    /**
     * Redo the operation corresponding to a given log record.
     * 
     * @param logRecordHelper
     *            (@see ILogRecordHelper) An implementation of the
     *            ILogRecordHelper interface that is used to parse the log
     *            record and extract useful information from the content.
     * @param LogicalLogLocator
     *            (@see LogicalLogLocator) The locationof the log record that
     *            needs to be undone.
     * @throws ACIDException
     */
    public void redo(ILogRecordHelper logRecordHelper, LogicalLogLocator memLSN) throws ACIDException;

}
