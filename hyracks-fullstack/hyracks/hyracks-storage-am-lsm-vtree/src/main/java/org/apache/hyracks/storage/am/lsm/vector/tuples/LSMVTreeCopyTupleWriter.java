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

import static org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference.ANTIMATTER_BIT_OFFSET;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.util.BitOperationUtils;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMTreeTupleReference;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Tuple writer that preserves the antimatter bit of the SOURCE tuple (LSMBTreeCopyTupleWriter
 * pattern). Used by merges with {@code returnDeletedTuples=true} (merging set excludes the oldest
 * disk component): preserved antimatter tuples drained from the merge cursor must be re-written
 * as antimatter, not silently re-encoded as matter. Matter tuples are written identically to
 * {@link LSMVTreeDataTupleWriter}.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public class LSMVTreeCopyTupleWriter extends LSMVTreeDataTupleWriter {

    public LSMVTreeCopyTupleWriter(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        super(typeTraits, nullTypeTraits, nullIntrospector);
    }

    @Override
    public int writeTuple(ITupleReference tuple, byte[] targetBuf, int targetOff) {
        // The copy writer's contract is to PRESERVE the source polarity during merge. A source tuple that
        // is not an ILSMTreeTupleReference would silently be written as matter (delete resurrection) — the
        // exact bug class this writer guards against — so assert the merge always feeds marker-aware tuples.
        assert tuple instanceof ILSMTreeTupleReference : "copy writer given a non-ILSMTreeTupleReference "
                + "source; delete-marker polarity cannot be preserved";
        int bytesWritten = super.writeTuple(tuple, targetBuf, targetOff);
        if (tuple instanceof ILSMTreeTupleReference && ((ILSMTreeTupleReference) tuple).isAntimatter()) {
            BitOperationUtils.setBit(targetBuf, targetOff, ANTIMATTER_BIT_OFFSET);
        }
        return bytesWritten;
    }
}
