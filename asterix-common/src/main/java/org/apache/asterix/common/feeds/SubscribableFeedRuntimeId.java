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
package org.apache.asterix.common.feeds;

import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.ActiveRuntimeId;
import org.apache.asterix.common.active.api.IActiveRuntime.ActiveRuntimeType;

public class SubscribableFeedRuntimeId extends ActiveRuntimeId {

    public static final long serialVersionUID = 1L;

    private final ActiveObjectId feedId;

    public SubscribableFeedRuntimeId(ActiveObjectId feedId, ActiveRuntimeType runtimeType, int partition) {
        super(runtimeType, partition, ActiveRuntimeId.DEFAULT_OPERAND_ID);
        this.feedId = feedId;
    }

    public ActiveObjectId getFeedId() {
        return feedId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SubscribableFeedRuntimeId)) {
            return false;
        }

        return (super.equals(o) && this.feedId.equals(((SubscribableFeedRuntimeId) o).getFeedId()));
    }

    @Override
    public int hashCode() {
        return super.hashCode() + feedId.hashCode();
    }
}
