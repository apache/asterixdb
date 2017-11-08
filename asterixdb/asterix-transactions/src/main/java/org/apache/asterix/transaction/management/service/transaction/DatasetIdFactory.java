/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.transaction.management.service.transaction;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class DatasetIdFactory {
    private static int id = 0;
    private static boolean isInitialized = false;

    public static synchronized boolean isInitialized() {
        return isInitialized;
    }

    public static synchronized void initialize(int initialId) {
        id = initialId;
        isInitialized = true;
    }

    public static synchronized int generateDatasetId() throws AlgebricksException {
        if (id == Integer.MAX_VALUE) {
            throw new AsterixException(ErrorCode.DATASET_ID_EXHAUSTED);
        }
        return ++id;
    }

    public static int generateAlternatingDatasetId(int originalId) {
        return originalId ^ 0x80000000;
    }

    public static synchronized int getMostRecentDatasetId() {
        return id;
    }
}
