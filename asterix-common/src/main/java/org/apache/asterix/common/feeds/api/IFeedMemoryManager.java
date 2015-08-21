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

import edu.uci.ics.asterix.common.feeds.api.IFeedMemoryComponent.Type;

/**
 * Provides management of memory allocated for handling feed data flow through the node controller
 */
public interface IFeedMemoryManager {

    public static final int START_COLLECTION_SIZE = 20;
    public static final int START_POOL_SIZE = 10;

    /**
     * Gets a memory component allocated from the feed memory budget
     * 
     * @param type
     *            the kind of memory component that needs to be allocated
     * @return
     * @see Type
     */
    public IFeedMemoryComponent getMemoryComponent(Type type);

    /**
     * Expand a memory component by the default increment
     * 
     * @param memoryComponent
     * @return true if the expansion succeeded
     *         false if the requested expansion violates the configured budget
     */
    public boolean expandMemoryComponent(IFeedMemoryComponent memoryComponent);

    /**
     * Releases the given memory component to reclaim the memory allocated for the component
     * 
     * @param memoryComponent
     *            the memory component that is being reclaimed/released
     */
    public void releaseMemoryComponent(IFeedMemoryComponent memoryComponent);

}
