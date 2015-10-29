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
package org.apache.asterix.common.channels;

import org.apache.asterix.common.active.ActiveObjectId;
import org.apache.asterix.common.active.ActiveObjectId.ActiveObjectType;
import org.apache.asterix.common.feeds.api.ActiveRuntimeId;
import org.apache.asterix.common.feeds.api.IActiveRuntime.ActiveRuntimeType;

/*
 * Since there is a single runtime for the procedure/channel,
 * All the runtimeid needs is name and dataverse
 */

public class ProcedureRuntimeId extends ActiveRuntimeId {

    public static final long serialVersionUID = 1L;
    protected final ActiveObjectId activeId;

    public ProcedureRuntimeId(ActiveObjectId activeId) {
        super(ActiveRuntimeType.REPETITIVE, 0, DEFAULT_OPERAND_ID);
        this.activeId = activeId;
    }

    public ProcedureRuntimeId(String dataverse, String name) {
        super(ActiveRuntimeType.REPETITIVE, 0, DEFAULT_OPERAND_ID);
        this.activeId = new ActiveObjectId(dataverse, name, ActiveObjectType.CHANNEL);
    }

    public ActiveObjectId getActiveId() {
        return activeId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ProcedureRuntimeId)) {
            return false;
        }

        if (this == o || ((ProcedureRuntimeId) o).getActiveId().equals(activeId)) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "RuntimeId for :" + activeId.toString();
    }

}
