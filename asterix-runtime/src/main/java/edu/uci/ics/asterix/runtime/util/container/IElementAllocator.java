/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.runtime.util.container;

/**
 * A reusable object pool interface
 */
public interface IElementAllocator<E, T> {

    /**
     * Give client an E instance
     * 
     * @param arg
     *            , the argument to create E
     * @return a E instance
     */
    public E allocate(T arg);

    /**
     * Clean all the used(assigned) instances in the pool. Then all instances in
     * the pool are marked as "unused" and can be returned to next bunch of
     * allocate call from a client
     */
    public void reset();
}
