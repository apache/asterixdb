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
package edu.uci.ics.asterix.transaction.management.logging;

import java.util.Random;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.IBuffer;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogger;
import edu.uci.ics.asterix.transaction.management.service.logging.IndexLogger.ReusableLogContentObject;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

public class BasicLogger implements ILogger {

    private static long averageContentCreationTime = 0;
    private static long count = 0;

    public void log(TransactionContext context, LogicalLogLocator wMemLSN, int length,
            ReusableLogContentObject reusableLogContentObject) throws ACIDException {

        byte[] logContent = getRandomBytes(length);
        try {
            long startTime2 = System.nanoTime();

            IBuffer buffer = (IBuffer) (wMemLSN.getBuffer());

            /*
             * synchronized(buffer){ buffer.position(wMemLSN.getMemoryOffset());
             * buffer.put(logContent); }
             */

            byte[] logPageStorage = buffer.getArray();
            System.arraycopy(logContent, 0, logPageStorage, wMemLSN.getMemoryOffset(), logContent.length);

            /*
             * for(int i=0;i<logContent.length;i++){
             * ((IFileBasedBuffer)(wMemLSN.
             * getBuffer())).put(wMemLSN.getMemoryOffset() + i, logContent[i]);
             * }
             */
            long endTime2 = System.nanoTime();
            averageContentCreationTime = ((averageContentCreationTime * count) + (endTime2 - startTime2)) / (++count);
        } catch (Exception e) {
            throw new ACIDException("", e);
        }
    }

    public static long getAverageContentCreationTime() {
        return averageContentCreationTime;
    }

    public static long getNumLogs() {
        return averageContentCreationTime;
    }

    public void postLog(TransactionContext context, ReusableLogContentObject reusableLogContentObject) throws ACIDException {
        // TODO Auto-generated method stub

    }

    public void preLog(TransactionContext context, ReusableLogContentObject reusableLogContentObject) throws ACIDException {
        // TODO Auto-generated method stub

    }

    private static byte[] getRandomBytes(int size) {
        byte[] b = new byte[size];
        Random random = new Random();
        int num = random.nextInt(30);
        Integer number = (new Integer(num + 65));
        byte numByte = number.byteValue();
        for (int i = 0; i < size; i++) {
            b[i] = numByte;
        }
        return b;
    }
}
