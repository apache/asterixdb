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
package org.apache.asterix.runtime.message;

import org.apache.asterix.common.replication.INCLifecycleMessage;

public abstract class AbstractFailbackPlanMessage implements INCLifecycleMessage {

    private static final long serialVersionUID = 1L;
    protected final long planId;
    protected final int requestId;

    public AbstractFailbackPlanMessage(long planId, int requestId) {
        this.planId = planId;
        this.requestId = requestId;
    }

    public long getPlanId() {
        return planId;
    }

    public int getRequestId() {
        return requestId;
    }
}
