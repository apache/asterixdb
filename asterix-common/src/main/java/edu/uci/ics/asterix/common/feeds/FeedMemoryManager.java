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
package edu.uci.ics.asterix.common.feeds;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.common.config.AsterixFeedProperties;
import edu.uci.ics.asterix.common.feeds.api.IFeedMemoryComponent;
import edu.uci.ics.asterix.common.feeds.api.IFeedMemoryComponent.Type;
import edu.uci.ics.asterix.common.feeds.api.IFeedMemoryManager;

public class FeedMemoryManager implements IFeedMemoryManager {

    private static final Logger LOGGER = Logger.getLogger(FeedMemoryManager.class.getName());
    private static final int ALLOCATION_INCREMENT = 10;

    private final AtomicInteger componentId = new AtomicInteger(0);
    private final String nodeId;
    private final int budget;
    private final int frameSize;

    private int committed;

    public FeedMemoryManager(String nodeId, AsterixFeedProperties feedProperties, int frameSize) {
        this.nodeId = nodeId;
        this.frameSize = frameSize;
        budget = (int) feedProperties.getMemoryComponentGlobalBudget() / frameSize;
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Feed Memory budget " + budget + " frames (frame size=" + frameSize + ")");
        }
    }

    @Override
    public synchronized IFeedMemoryComponent getMemoryComponent(Type type) {
        IFeedMemoryComponent memoryComponent = null;
        boolean valid = false;
        switch (type) {
            case COLLECTION:
                valid = committed + START_COLLECTION_SIZE <= budget;
                if (valid) {
                    memoryComponent = new FrameCollection(componentId.incrementAndGet(), this, START_COLLECTION_SIZE);
                }
                break;
            case POOL:
                valid = committed + START_POOL_SIZE <= budget;
                if (valid) {
                    memoryComponent = new DataBucketPool(componentId.incrementAndGet(), this, START_POOL_SIZE,
                            frameSize);
                }
                committed += START_POOL_SIZE;
                break;
        }
        if (!valid) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Unable to allocate memory component of type" + type);
            }
        }
        return valid ? memoryComponent : null;
    }

    @Override
    public synchronized boolean expandMemoryComponent(IFeedMemoryComponent memoryComponent) {
        if (committed + ALLOCATION_INCREMENT > budget) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.warning("Memory budget " + budget + " is exhausted. Space left: " + (budget - committed)
                        + " frames.");
            }
            return false;
        } else {
            memoryComponent.expand(ALLOCATION_INCREMENT);
            committed += ALLOCATION_INCREMENT;
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Expanded memory component " + memoryComponent + " by " + ALLOCATION_INCREMENT + " " + this);
            }
            return true;
        }
    }

    @Override
    public synchronized void releaseMemoryComponent(IFeedMemoryComponent memoryComponent) {
        int delta = memoryComponent.getTotalAllocation();
        committed -= delta;
        memoryComponent.reset();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Reset " + memoryComponent + " and reclaimed " + delta + " frames " + this);
        }
    }

    @Override
    public String toString() {
        return "FeedMemoryManager  [" + nodeId + "]" + "(" + committed + "/" + budget + ")";
    }

}
