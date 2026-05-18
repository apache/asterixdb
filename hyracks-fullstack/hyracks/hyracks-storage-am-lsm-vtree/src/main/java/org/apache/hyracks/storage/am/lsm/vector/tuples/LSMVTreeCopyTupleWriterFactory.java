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

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.INullIntrospector;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Factory for {@link LSMVTreeCopyTupleWriter} instances — merge-path data-frame writers that
 * carry over the source tuple's antimatter bit.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public class LSMVTreeCopyTupleWriterFactory extends LSMVTreeDataTupleWriterFactory {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final ITypeTraits nullTypeTraits;
    private final INullIntrospector nullIntrospector;

    public LSMVTreeCopyTupleWriterFactory(ITypeTraits[] typeTraits, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        super(typeTraits, false, nullTypeTraits, nullIntrospector);
        this.typeTraits = typeTraits;
        this.nullTypeTraits = nullTypeTraits;
        this.nullIntrospector = nullIntrospector;
    }

    @Override
    public ITreeIndexTupleWriter createTupleWriter() {
        return new LSMVTreeCopyTupleWriter(typeTraits, nullTypeTraits, nullIntrospector);
    }
}
