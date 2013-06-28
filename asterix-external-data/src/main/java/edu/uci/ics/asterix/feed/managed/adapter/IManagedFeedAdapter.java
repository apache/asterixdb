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
package edu.uci.ics.asterix.feed.managed.adapter;

import java.util.Map;

/**
 * Interface implemented by an adapter that can be controlled or managed by external
 * commands (stop,alter)
 */
public interface IManagedFeedAdapter {

    /**
     * Discontinue the ingestion of data and end the feed.
     * 
     * @throws Exception
     */
    public void stop();

    /**
     * Modify the adapter configuration parameters. This method is called
     * when the configuration parameters need to be modified while the adapter
     * is ingesting data in an active feed.
     * 
     * @param properties
     *            A HashMap containing the set of configuration parameters
     *            that need to be altered.
     */
    public void alter(Map<String, String> properties);

}
