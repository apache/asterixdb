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
package org.apache.hyracks.storage.common.buffercache;

public interface IPageWriteFailureCallback {

    /**
     * Notify that an async write operation has failed
     *
     * @param page
     * @param failure
     */
    void writeFailed(ICachedPage page, Throwable failure);

    /**
     * @return true if the callback has received any failure
     */
    boolean hasFailed();

    /**
     * @return a failure writing to disk or null if no failure has been seen
     *         This doesn't guarantee which failure is returned but that if one or more failures occurred
     *         while trying to write to disk, one of those failures is returned. All other failures are expected
     *         to be logged.
     */
    Throwable getFailure();
}
