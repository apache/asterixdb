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

/**
 * Represents an in-memory components required for storing frames that contain feed tuples.
 * The component's memory footprint is measured and regulated by the {@link IFeedMemoryManager}.
 * Any expansion in size is accounted and can be restricted by the {@link IFeedMemoryManager}
 **/
public interface IFeedMemoryComponent {

    public enum Type {

        /** A pool of reusable frames **/
        POOL,

        /** An ordered list of frames **/
        COLLECTION
    }

    /** Gets the unique id associated with the memory component **/
    public int getComponentId();

    /** Gets the type associated with the component. **/
    public Type getType();

    /** Gets the current size (number of allocated frames) of the component. **/
    public int getTotalAllocation();

    /**
     * Expands this memory component by the speficied number of frames
     * 
     * @param delta
     *            the amount (measured in number of frames) by which this memory component
     *            should be expanded
     */
    public void expand(int delta);

    /** Clears the allocated frames as a step to reclaim the memory **/
    public void reset();

}
