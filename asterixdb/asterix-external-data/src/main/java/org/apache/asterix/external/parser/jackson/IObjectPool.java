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
package org.apache.asterix.external.parser.jackson;

/**
 * Object pool for DFS traversal mode, which allows to recycle objects
 * as soon as it is not needed.
 */
public interface IObjectPool<E> {
    E getInstance();

    void recycle(E object);

    /**
     * Makes the pool's current size the one {@link #getPooledBytes()} reports. For the pool's owner to call when it
     * has returned everything it borrowed: a borrowed object is not counted, so the size is too low at any other time.
     */
    void publishPooledBytes();

    /**
     * @return the bytes held by the pooled objects when the size was last {@link #publishPooledBytes() published},
     *         less what the garbage collector has reclaimed since; safe to call from any thread
     */
    long getPooledBytes();
}
