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
package org.apache.asterix.common.feeds.api;

/**
 * Interface implemented by a feed adapter.
 */
public interface IFeedAdapter extends IDatasourceAdapter {

    public enum DataExchangeMode {
        /**
         * PULL model requires the adaptor to make a separate request each time to receive data
         **/
        PULL,

        /**
         * PUSH mode involves the use o just one initial request (handshake) by the adaptor
         * to the datasource for setting up the connection and providing any protocol-specific
         * parameters. Once a connection is established, the data source "pushes" data to the adaptor.
         **/
        PUSH
    }

    /**
     * Returns the data exchange mode (PULL/PUSH) associated with the flow.
     * 
     * @return
     */
    public DataExchangeMode getDataExchangeMode();

    /**
     * Discontinue the ingestion of data and end the feed.
     * 
     * @throws Exception
     */
    public void stop() throws Exception;

    /**
     * @param e
     * @return true if the feed ingestion should continue post the exception else false
     */
    public boolean handleException(Exception e);

}
