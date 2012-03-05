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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.util.Map;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

/**
 * An interface providing call back APIs that are invoked {@link ILogManager} for providing the content for the log record and doing any pre/post
 * processing.
 */
public interface ILogger {

    public void preLog(TransactionContext context, Map<Object, Object> loggerArguments) throws ACIDException;

    public void log(TransactionContext context, final LogicalLogLocator logicalLogLocator, int logRecordSize,
            Map<Object, Object> loggerArguments) throws ACIDException;

    public void postLog(TransactionContext context, Map<Object, Object> loggerArguments) throws ACIDException;

}
