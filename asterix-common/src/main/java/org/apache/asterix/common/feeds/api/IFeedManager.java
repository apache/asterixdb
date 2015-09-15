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

import org.apache.asterix.common.config.AsterixFeedProperties;
import org.apache.asterix.common.feeds.api.IFeedConnectionManager;
import org.apache.asterix.common.feeds.api.IFeedMemoryManager;
import org.apache.asterix.common.feeds.api.IFeedMessageService;
import org.apache.asterix.common.feeds.api.IFeedMetadataManager;
import org.apache.asterix.common.feeds.api.IFeedMetricCollector;
import org.apache.asterix.common.feeds.api.IFeedSubscriptionManager;

/**
 * Provides access to services related to feed management within a node controller
 */
public interface IFeedManager {

    /**
     * gets the nodeId associated with the host node controller
     * 
     * @return the nodeId associated with the host node controller
     */
    public String getNodeId();

    /**
     * gets the handle to the singleton instance of subscription manager
     * 
     * @return the singleton instance of subscription manager
     * @see IFeedSubscriptionManager
     */
    public IFeedSubscriptionManager getFeedSubscriptionManager();

    /**
     * gets the handle to the singleton instance of connection manager
     * 
     * @return the singleton instance of connection manager
     * @see IFeedConnectionManager
     */
    public IFeedConnectionManager getFeedConnectionManager();

    /**
     * gets the handle to the singleton instance of memory manager
     * 
     * @return the singleton instance of memory manager
     * @see IFeedMemoryManager
     */
    public IFeedMemoryManager getFeedMemoryManager();

    /**
     * gets the handle to the singleton instance of feed metadata manager
     * 
     * @return the singleton instance of feed metadata manager
     * @see IFeedMetadataManager
     */
    public IFeedMetadataManager getFeedMetadataManager();

    /**
     * gets the handle to the singleton instance of feed metric collector
     * 
     * @return the singleton instance of feed metric collector
     * @see IFeedMetricCollector
     */
    public IFeedMetricCollector getFeedMetricCollector();

    /**
     * gets the handle to the singleton instance of feed message service
     * 
     * @return the singleton instance of feed message service
     * @see IFeedMessageService
     */
    public IFeedMessageService getFeedMessageService();

    /**
     * gets the asterix configuration
     * 
     * @return asterix configuration
     * @see AsterixFeedProperties
     */
    public AsterixFeedProperties getAsterixFeedProperties();

}
