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

package edu.uci.ics.asterix.transaction.management.resource;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

/**
 * Represents a closeable resource that implements a close(@see
 * TransactionContext) method. When a transaction commits/aborts, the close
 * method is called on each of the ICloseable resources that were involved in
 * the transaction. This gives an opportunity to release all resources and do a
 * cleanup. An example of ICloseable is the @see TreeLogger.
 */
public interface ICloseable {

    /**
     * This method is invoked at the commit/abort of a transaction that involved
     * a ICloseable resource. It is used to do a clean up by the involved
     * resource before the transaction ends.
     * 
     * @param context
     * @throws ACIDException
     */
    public void close(TransactionContext context) throws ACIDException;

}
