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
package org.apache.asterix.common.context;

/**
 * Told of every index the {@link DatasetLifecycleManager} opens that was not already open, for metrics. Invoked on the
 * opening thread while the dataset lifecycle manager is locked, so it must not block.
 */
@FunctionalInterface
public interface IIndexOpenListener {
    IIndexOpenListener NO_OP = durationNanos -> {
    };

    /**
     * @param durationNanos how long activating the index took, including waiting for its operation tracker
     */
    void indexOpened(long durationNanos);
}
