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
package org.apache.asterix.common.vector;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * The quantization schemes a VTree index can encode its embeddings with, together with the bit width each
 * one quantizes to. Sibling of {@link VectorSimilarityMetric}: the single source of truth for the values the
 * index {@code quantization} option accepts.
 * <p>
 * The bit width is what actually reaches the storage layer — as {@code OptimizedScalarQuantizationCodec.Params}
 * and as a field on the local resource — so it belongs on the constant rather than in a lookup that has to
 * decide what an unrecognized label means.
 * <p>
 * {@link #label()} is the spelling written in DDL and <em>persisted</em> in the {@code Metadata.Index} record.
 * It is {@link #name()} today, which means renaming a constant strands existing indexes: add a constant rather
 * than renaming one, and if a name ever has to change, give {@code label()} its own string instead.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED)
public enum VectorQuantization {
    SQ4(4),
    SQ8(8);

    private final int bits;

    VectorQuantization(int bits) {
        this.bits = bits;
    }

    /** Bits per component this scheme quantizes an embedding to. */
    public int bits() {
        return bits;
    }

    /** The DDL spelling, and the form persisted in index metadata. */
    public String label() {
        return name();
    }

    /**
     * Resolves a {@code quantization} label, trimmed and case-insensitively.
     *
     * @return the matching scheme, or {@code null} if the label is not recognized.
     */
    public static VectorQuantization fromLabel(String label) {
        if (label == null) {
            return null;
        }
        String normalized = label.trim().toUpperCase(Locale.ROOT);
        for (VectorQuantization quantization : values()) {
            if (quantization.label().equals(normalized)) {
                return quantization;
            }
        }
        return null;
    }

    /** Comma-separated labels, for "allowed values are ..." diagnostics. */
    public static String labelList() {
        return Arrays.stream(values()).map(VectorQuantization::label).collect(Collectors.joining(", "));
    }
}
