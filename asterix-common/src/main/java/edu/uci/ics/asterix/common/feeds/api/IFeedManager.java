/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.common.feeds.api;

import edu.uci.ics.asterix.common.config.AsterixFeedProperties;
import edu.uci.ics.asterix.common.feeds.api.IFeedConnectionManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMemoryManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMessageService;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetadataManager;
import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector;
import edu.uci.ics.asterix.common.feeds.api.IFeedSubscriptionManager;

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
