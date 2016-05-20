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

public class LogSource {

    public static final byte LOCAL = 0;
    public static final byte REMOTE = 1;

    private static final String STRING_LOCAL = "LOCAL";
    private static final String STRING_REMOTE = "REMOTE";

    private static final String STRING_INVALID_LOG_SOURCE = "INVALID_LOG_SOURCE";

    public static String toString(byte LogSource) {
        switch (LogSource) {
            case LOCAL:
                return STRING_LOCAL;
            case REMOTE:
                return STRING_REMOTE;
            default:
                return STRING_INVALID_LOG_SOURCE;
        }
    }

}
