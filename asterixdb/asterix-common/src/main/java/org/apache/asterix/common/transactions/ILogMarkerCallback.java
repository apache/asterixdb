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

import java.nio.ByteBuffer;

/**
 * This interface provide callback mechanism for adding marker logs to the transaction log file
 */
public interface ILogMarkerCallback {

    String KEY_MARKER_CALLBACK = "MARKER_CALLBACK";

    /**
     * Called before writing the marker log allowing addition of specific information to the log record
     *
     * @param buffer:
     *            the log buffer to write to
     */
    void before(ByteBuffer buffer);

    /**
     * Called after the log's been appended to the log tail passing the position of the log used for random access
     *
     * @param lsn
     */
    void after(long lsn);

}
