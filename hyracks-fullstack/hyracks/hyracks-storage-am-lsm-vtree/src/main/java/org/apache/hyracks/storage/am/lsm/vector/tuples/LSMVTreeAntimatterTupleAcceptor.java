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

package org.apache.hyracks.storage.am.lsm.vector.tuples;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.btree.api.ITupleAcceptor;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Accepts the tuples a same-key write may overwrite, namely delete markers. The VTree counterpart of
 * {@code AntimatterAwareTupleAcceptor}: it lets the tree layer ask whether it may overwrite a tuple
 * without knowing what antimatter is, since the encoding belongs to the LSM tuple writer. A tuple that
 * is not an {@link ILSMTreeTupleReference} carries no antimatter bit and counts as matter.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_CLI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public enum LSMVTreeAntimatterTupleAcceptor implements ITupleAcceptor {
    INSTANCE;

    @Override
    public boolean accept(ITupleReference tuple) {
        if (tuple == null) {
            return true;
        }
        return tuple instanceof ILSMTreeTupleReference && ((ILSMTreeTupleReference) tuple).isAntimatter();
    }
}
