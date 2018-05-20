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
package org.apache.asterix.common.transactions;

public class LogType {

    public static final byte UPDATE = 0;
    public static final byte JOB_COMMIT = 1;
    public static final byte ENTITY_COMMIT = 2;
    public static final byte ABORT = 3;
    public static final byte FLUSH = 4;
    public static final byte WAIT = 6;
    public static final byte FILTER = 7;
    public static final byte MARKER = 8;
    public static final byte WAIT_FOR_FLUSHES = 9;

    private static final String STRING_UPDATE = "UPDATE";
    private static final String STRING_JOB_COMMIT = "JOB_COMMIT";
    private static final String STRING_ENTITY_COMMIT = "ENTITY_COMMIT";
    private static final String STRING_ABORT = "ABORT";
    private static final String STRING_FLUSH = "FLUSH";
    private static final String STRING_WAIT = "WAIT";
    private static final String STRING_FILTER = "FILTER";
    private static final String STRING_MARKER = "MARKER";
    private static final String STRING_WAIT_FOR_FLUSHES = "WAIT_FOR_FLUSHES";
    private static final String STRING_UNKNOWN_LOG_TYPE = "UNKNOWN_LOG_TYPE";

    public static String toString(byte logType) {
        switch (logType) {
            case LogType.UPDATE:
                return STRING_UPDATE;
            case LogType.JOB_COMMIT:
                return STRING_JOB_COMMIT;
            case LogType.ENTITY_COMMIT:
                return STRING_ENTITY_COMMIT;
            case LogType.ABORT:
                return STRING_ABORT;
            case LogType.FLUSH:
                return STRING_FLUSH;
            case LogType.WAIT:
                return STRING_WAIT;
            case LogType.WAIT_FOR_FLUSHES:
                return STRING_WAIT_FOR_FLUSHES;
            case LogType.FILTER:
                return STRING_FILTER;
            case LogType.MARKER:
                return STRING_MARKER;
            default:
                return STRING_UNKNOWN_LOG_TYPE;
        }
    }

}
