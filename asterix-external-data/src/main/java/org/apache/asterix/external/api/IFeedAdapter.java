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
package org.apache.asterix.external.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public interface IFeedAdapter extends IDataSourceAdapter {
    /**
     * Pause the ingestion of data.
     * @throws HyracksDataException
     * @throws Exception
     */
    public boolean pause() throws HyracksDataException;

    /**
     * Resume the ingestion of data.
     * @throws HyracksDataException
     * @throws Exception
     */
    public boolean resume() throws HyracksDataException;

    /**
     * Discontinue the ingestion of data.
     * @throws Exception
     */
    public boolean stop() throws Exception;

    /**
     * @param e
     * @return true if the ingestion should continue post the exception else false
     * @throws Exception
     */
    public boolean handleException(Throwable e);
}
