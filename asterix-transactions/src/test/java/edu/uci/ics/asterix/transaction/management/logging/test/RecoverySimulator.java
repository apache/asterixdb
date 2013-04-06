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
package edu.uci.ics.asterix.transaction.management.logging.test;

import java.io.IOException;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.recovery.IRecoveryManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionSubsystem;

public class RecoverySimulator {

    private static IRecoveryManager recoveryManager;

    public static void startRecovery() throws IOException, ACIDException {
        recoveryManager.startRecovery(true);
    }

    public static void main(String args[]) throws IOException, ACIDException {
        String id = "nc1";
        try {
            TransactionSubsystem factory = new TransactionSubsystem(id, null);
            IRecoveryManager recoveryManager = factory.getRecoveryManager();
            recoveryManager.startRecovery(true);
        } catch (ACIDException acide) {
            acide.printStackTrace();
            throw acide;
        }
    }
}
