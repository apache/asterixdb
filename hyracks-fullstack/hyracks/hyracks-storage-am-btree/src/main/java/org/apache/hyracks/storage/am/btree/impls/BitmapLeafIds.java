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
package org.apache.hyracks.storage.am.btree.impls;

import org.apache.hyracks.util.annotations.AiProvenance;

/**
 * Succinct, random-access set of B-tree leaf page ids for LSM sampling (ASTERIXDB-3702). Replaces the raw
 * {@code int[]} of leaf page ids that {@link DiskBTree#enumerateLeafPageIds} used to materialize
 * (one {@code int} per leaf = {@code 4 * N_leaf} bytes; ~132 MB for a 33M-leaf row component).
 * <p>
 * Leaf page ids in an append-only bulk-loaded row disk component are dense in {@code [0, maxPageId]}
 * (leaves are ~99% of pages; interiors are the rest), so a plain bitmap over that universe plus a
 * sampled select index is ~2 bits/elem — ~15x smaller than the {@code int[]} — while giving the only
 * operations the sampler needs: {@link #size()} and {@link #get(int)} (the i-th leaf id, in ascending
 * page-id order, identical to {@code leafPageIds[i]} since leaves are enumerated in key/DFS order).
 * <p>
 * Built by streaming: {@link #set(int)} per enumerated leaf id (no intermediate array), then
 * {@link #build()} once. Not thread-safe; single-writer during enumeration, read-only after build.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Bitmap+select encoding of leaf page ids to replace the int[] (ASTERIXDB-3702)")
public final class BitmapLeafIds {

    // Every S-th set bit gets a sampled entry (word index + ones-before) so select() scans at most
    // ~S bits. 64 keeps the index at ~1 bit/elem, matching the bitmap's ~1 bit/elem (dense) → ~2 total.
    private static final int SELECT_SAMPLE = 64;

    private final long[] bits;
    private int count;
    // sampled select index, populated in build()
    private int[] sampleWord;
    private int[] sampleOnesBefore;
    // Lifecycle: set() during enumeration, then build() once, then get() reads. Guards below enforce it.
    private boolean built;

    public BitmapLeafIds(long universe) {
        if (universe < 0) {
            universe = 0;
        }
        this.bits = new long[(int) ((universe + 63) / 64)];
    }

    /** Record a leaf page id. Ids must be within {@code [0, universe)}. Cold path (once per enumerated leaf). */
    public void set(int id) {
        if (built) {
            throw new IllegalStateException("set() after build()");
        }
        int w = id >>> 6;
        long mask = 1L << (id & 63);
        if ((bits[w] & mask) == 0) {
            bits[w] |= mask;
            count++;
        }
    }

    /** Finalize the select index. Call once after all {@link #set(int)} calls. */
    public void build() {
        int ns = count / SELECT_SAMPLE + 1;
        sampleWord = new int[ns];
        sampleOnesBefore = new int[ns];
        int seen = 0;
        int next = 0;
        for (int w = 0; w < bits.length && next < ns; w++) {
            int pc = Long.bitCount(bits[w]);
            while (next < ns && (long) next * SELECT_SAMPLE < seen + pc) {
                sampleWord[next] = w;
                sampleOnesBefore[next] = seen;
                next++;
            }
            seen += pc;
        }
        while (next < ns) {
            sampleWord[next] = bits.length == 0 ? 0 : bits.length - 1;
            sampleOnesBefore[next] = count;
            next++;
        }
        built = true;
    }

    /** Number of leaf ids. */
    public int size() {
        return count;
    }

    /** The i-th smallest leaf id (0-based), i.e. the position of the i-th set bit. */
    public int get(int i) {
        // Preconditions asserted (not thrown): asserts strip out in production, so the per-draw hot path
        // keeps no branch, while build()-before-get and out-of-range misuse are caught in tests.
        assert built : "get() before build()";
        assert i >= 0 && i < count : "leaf index out of range: " + i + " of " + count;
        int s = i / SELECT_SAMPLE;
        int wi = sampleWord[s];
        int seen = sampleOnesBefore[s];
        long w = bits[wi];
        while (true) {
            int pc = Long.bitCount(w);
            if (i - seen < pc) {
                return wi * 64 + selectInWord(w, i - seen);
            }
            seen += pc;
            wi++;
            w = bits[wi];
        }
    }

    // position of the j-th (0-based) set bit within a single word
    private static int selectInWord(long w, int j) {
        for (; j > 0; j--) {
            w &= w - 1; // clear lowest set bit
        }
        return Long.numberOfTrailingZeros(w);
    }
}
