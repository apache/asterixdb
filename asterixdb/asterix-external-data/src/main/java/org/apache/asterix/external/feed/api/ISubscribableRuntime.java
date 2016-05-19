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
package org.apache.asterix.external.feed.api;

import org.apache.asterix.external.feed.runtime.CollectionRuntime;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Represent a feed runtime whose output can be routed along other parallel path(s).
 */
public interface ISubscribableRuntime extends IFeedRuntime {

    /**
     * @param collectionRuntime
     * @throws Exception
     */
    public void subscribe(CollectionRuntime collectionRuntime) throws HyracksDataException;

    /**
     * @param collectionRuntime
     * @throws InterruptedException
     * @throws Exception
     */
    public void unsubscribe(CollectionRuntime collectionRuntime) throws HyracksDataException, InterruptedException;
}
