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
package org.apache.asterix.common.storage;

/**
 * How a secondary index's creation is completed, decided by whoever builds its creation job. An index that is still
 * being created must be told apart from one whose first flush was lost, or local recovery rolls the whole partition,
 * primary included, back to NOT_FOUND (ASTERIXDB-3839); and one whose creation died must be reclaimed by storage
 * cleanup. Both are driven by a pending-creation flag on the index's checkpoint, owned by its
 * {@link IIndexCheckpointManager}.
 */
public enum IndexCompletionMode {
    /**
     * Creation completes when the load job that follows lands its partition. The index is created pending, so it
     * never exists on disk empty and unflagged.
     */
    ON_LOAD,
    /**
     * Creation completes with the creation job itself: nothing follows it, and the index starts empty and is filled
     * by ingestion.
     */
    ON_CREATE
}
