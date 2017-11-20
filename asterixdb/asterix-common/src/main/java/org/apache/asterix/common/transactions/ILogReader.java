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

public interface ILogReader {

    /**
     * Sets the log reader position at log sequence number with value {@code lsn}.
     *
     * @param lsn
     */
    void setPosition(long lsn);

    /**
     * Reads and returns the log record located at the log reader current position. After reading the log record,
     * the log reader position is incremented by the size of the read log.
     *
     * @return the log record
     */
    ILogRecord next();

    /**
     * Reads and returns the log record with log sequence number {@code lsn}.
     *
     * @param lsn
     * @return The log record
     */
    ILogRecord read(long lsn);

    /**
     * Closes the log reader and any resources used.
     */
    void close();

}