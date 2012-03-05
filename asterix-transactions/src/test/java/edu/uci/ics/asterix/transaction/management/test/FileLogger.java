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
package edu.uci.ics.asterix.transaction.management.test;

import java.util.Map;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.logging.IResource;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogger;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

class FileLogger implements ILogger {

    IResource resource;
    String logRecordContent;

    public FileLogger(IResource resource) {
        this.resource = resource;
    }

    public int generateLogRecordContent(int currentValue, int finalValue) {
        StringBuilder builder = new StringBuilder();
        builder.append("" + currentValue + " " + finalValue);
        logRecordContent = new String(builder);
        return resource.getId().length + logRecordContent.length();
    }

    @Override
    public void preLog(TransactionContext context, Map<Object, Object> loggerArguments) throws ACIDException {
        // TODO Auto-generated method stub

    }

    @Override
    public void log(TransactionContext context, final LogicalLogLocator memLSN, int logRecordSize,
            Map<Object, Object> loggerArguments) throws ACIDException {
        byte[] buffer = memLSN.getBuffer().getArray();
        byte[] content = logRecordContent.getBytes();
        for (int i = 0; i < resource.getId().length; i++) {
            buffer[memLSN.getMemoryOffset() + i] = resource.getId()[i];
        }
        for (int i = 0; i < content.length; i++) {
            buffer[memLSN.getMemoryOffset() + resource.getId().length + i] = content[i];
        }
    }

    @Override
    public void postLog(TransactionContext context, Map<Object, Object> loggerArguments) throws ACIDException {
        // TODO Auto-generated method stub

    }

}
