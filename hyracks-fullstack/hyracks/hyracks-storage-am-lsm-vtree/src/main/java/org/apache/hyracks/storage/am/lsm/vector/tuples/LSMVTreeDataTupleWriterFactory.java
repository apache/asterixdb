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
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriterFactory;
import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Factory for creating LSMVTreeDataTupleWriter instances.
 *
 * Used to create tuple writers for vector index data frames with anti-matter support.
 */
public class LSMVTreeDataTupleWriterFactory implements ITreeIndexTupleWriterFactory {

    private static final long serialVersionUID = 1L;

    private final ITypeTraits[] typeTraits;
    private final ITypeTraits nullTypeTraits;
    private final INullIntrospector nullIntrospector;
    private final boolean isAntimatter;

    public LSMVTreeDataTupleWriterFactory(ITypeTraits[] typeTraits, boolean isAntimatter, ITypeTraits nullTypeTraits,
            INullIntrospector nullIntrospector) {
        this.typeTraits = typeTraits;
        this.isAntimatter = isAntimatter;
        this.nullTypeTraits = nullTypeTraits;
        this.nullIntrospector = nullIntrospector;
    }

    @Override
    public ITreeIndexTupleWriter createTupleWriter() {
        LSMVTreeDataTupleWriter writer = new LSMVTreeDataTupleWriter(typeTraits, nullTypeTraits, nullIntrospector);
        writer.setAntimatter(isAntimatter);
        return writer;
    }

    /**
     * Derives a merge-path copy-writer factory over the same type traits: its writers preserve
     * the SOURCE tuple's antimatter bit instead of imposing this factory's fixed matter/antimatter
     * flag. Required when a merge preserves antimatter tuples (merging set excludes the oldest
     * disk component).
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_FABLE_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED)
    public LSMVTreeCopyTupleWriterFactory createCopyWriterFactory() {
        return new LSMVTreeCopyTupleWriterFactory(typeTraits, nullTypeTraits, nullIntrospector);
    }
}
