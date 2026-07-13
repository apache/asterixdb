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

package org.apache.hyracks.storage.am.lsm.btree;

import java.util.Random;

/**
 * Microbenchmark for compact encodings of a sorted, near-dense int list with random access by index
 * ({@code select(i)} = the i-th value). Motivated by {@code DiskBTree.enumerateLeafPageIds}, which returns an
 * {@code int[]} of every leaf page id so the sampler can draw the i-th leaf uniformly at random. That array is
 * ~4 * N_leaf bytes (a 1TB component is ~33MB at 128KB pages, ~128MB at 32KB pages); this harness explores
 * shrinking it without losing cheap {@code select(i)} (see the TODO on {@code enumerateLeafPageIds}).
 * <p>
 * Encoders (all dependency-free, verified against the baseline):
 * <ul>
 *   <li>{@code int[]} baseline — 32 bits/elem, ~0.6 ns select.</li>
 *   <li>Delta + VByte + block index — ~8 bits/elem, but O(block) select (cumulative gap decode).</li>
 *   <li>Frame-of-Reference bit-packing — ~8-9 bits/elem, O(1) select (~7 ns). Best latency after int[].</li>
 *   <li>Plain bitmap + sampled select — ~2 bits/elem (dense), ~31 ns select.</li>
 *   <li>Elias-Fano — ~3 bits/elem (dense), ~28 ns select, O(1) select structure. Best size+speed balance.</li>
 * </ul>
 * <b>RoaringBitmap</b> is intentionally not included here (it is not a hyracks dependency). Measured separately
 * (see {@code docs/RoarBench.java}): Roaring is the smallest (~0.65 bits/elem with {@code runOptimize}) but its
 * {@code select(j)} is 8-330x slower than the plain bitmap on this data — run-container {@code select} is ~O(#runs),
 * and holes fragment the data into many short runs. Roaring is built for set algebra/iteration, not point
 * {@code select}, so it is the wrong tool for this random-draw hot path despite the best footprint.
 * <p>
 * Not a unit test (no {@code @Test}) — run on demand:
 * {@code java -cp <test-classpath> org.apache.hyracks.storage.am.lsm.btree.LeafIdEncodingBenchmark [n] [densityPct]}
 */
public final class LeafIdEncodingBenchmark {

    private LeafIdEncodingBenchmark() {
    }

    interface Enc {
        String name();

        long sizeBytes();

        int select(int i);
    }

    // ---- 0. baseline int[] ----
    static final class Baseline implements Enc {
        final int[] a;

        Baseline(int[] a) {
            this.a = a;
        }

        public String name() {
            return "int[] baseline";
        }

        public long sizeBytes() {
            return 4L * a.length;
        }

        public int select(int i) {
            return a[i];
        }
    }

    // ---- 1. Delta + VByte with block skip index (small, but O(block) random access) ----
    static final class DeltaVByte implements Enc {
        static final int B = 128;
        final int[] blockFirst;
        final int[] blockOff;
        final byte[] stream;

        DeltaVByte(int[] a) {
            int n = a.length;
            int nb = (n + B - 1) / B;
            blockFirst = new int[nb];
            blockOff = new int[nb];
            java.io.ByteArrayOutputStream bo = new java.io.ByteArrayOutputStream();
            for (int b = 0; b < nb; b++) {
                int s = b * B;
                int e = Math.min(s + B, n);
                blockFirst[b] = a[s];
                blockOff[b] = bo.size();
                for (int i = s + 1; i < e; i++) {
                    writeVByte(bo, a[i] - a[i - 1]);
                }
            }
            stream = bo.toByteArray();
        }

        static void writeVByte(java.io.ByteArrayOutputStream bo, int v) {
            while ((v & ~0x7F) != 0) {
                bo.write((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            bo.write(v);
        }

        public String name() {
            return "Delta+VByte+blkIdx";
        }

        public long sizeBytes() {
            return (long) blockFirst.length * 4 + (long) blockOff.length * 4 + stream.length;
        }

        public int select(int i) {
            int b = i / B;
            int pos = i % B;
            int val = blockFirst[b];
            if (pos == 0) {
                return val;
            }
            int off = blockOff[b];
            for (int k = 0; k < pos; k++) {
                int shift = 0;
                int x = 0;
                int c;
                do {
                    c = stream[off++] & 0xFF;
                    x |= (c & 0x7F) << shift;
                    shift += 7;
                } while ((c & 0x80) != 0);
                val += x;
            }
            return val;
        }
    }

    // ---- bit I/O over a long[] ----
    static void setBits(long[] w, long bitPos, int len, long v) {
        if (len == 0) {
            return;
        }
        int wi = (int) (bitPos >> 6);
        int off = (int) (bitPos & 63);
        w[wi] |= (v << off);
        if (off + len > 64) {
            w[wi + 1] |= (v >>> (64 - off));
        }
    }

    static long getBits(long[] w, long bitPos, int len) {
        if (len == 0) {
            return 0;
        }
        int wi = (int) (bitPos >> 6);
        int off = (int) (bitPos & 63);
        long v = w[wi] >>> off;
        if (off + len > 64) {
            v |= w[wi + 1] << (64 - off);
        }
        return v & ((len == 64) ? -1L : ((1L << len) - 1));
    }

    // ---- 2. Frame-of-Reference bit-packing per block (O(1) select) ----
    static final class ForBitPack implements Enc {
        static final int B = 128;
        final int[] blockMin;
        final byte[] blockW;
        final long[] blockBitOff;
        final long[] words;

        ForBitPack(int[] a) {
            int n = a.length;
            int nb = (n + B - 1) / B;
            blockMin = new int[nb];
            blockW = new byte[nb];
            blockBitOff = new long[nb];
            long totalBits = 0;
            for (int b = 0; b < nb; b++) {
                int s = b * B;
                int e = Math.min(s + B, n);
                int mn = a[s];
                int mx = a[s];
                for (int i = s; i < e; i++) {
                    if (a[i] < mn) {
                        mn = a[i];
                    }
                    if (a[i] > mx) {
                        mx = a[i];
                    }
                }
                int range = mx - mn;
                int w = range == 0 ? 0 : (32 - Integer.numberOfLeadingZeros(range));
                blockMin[b] = mn;
                blockW[b] = (byte) w;
                blockBitOff[b] = totalBits;
                totalBits += (long) w * (e - s);
            }
            words = new long[(int) ((totalBits + 63) / 64)];
            for (int b = 0; b < nb; b++) {
                int s = b * B;
                int e = Math.min(s + B, n);
                int w = blockW[b];
                int mn = blockMin[b];
                long bit = blockBitOff[b];
                for (int i = s; i < e; i++) {
                    setBits(words, bit, w, a[i] - mn);
                    bit += w;
                }
            }
        }

        public String name() {
            return "FOR bit-pack (B=128)";
        }

        public long sizeBytes() {
            return (long) blockMin.length * 4 + blockW.length + (long) blockBitOff.length * 8 + (long) words.length * 8;
        }

        public int select(int i) {
            int b = i / B;
            int pos = i % B;
            int w = blockW[b];
            return blockMin[b] + (int) getBits(words, blockBitOff[b] + (long) pos * w, w);
        }
    }

    /** Sampled select over a bitvector: for every S-th one, store its word index and ones-before; then scan. */
    static final class SelectIndex {
        static final int S = 64;
        final long[] bits;
        final int[] sWord;
        final int[] sOnes;
        final int ones;

        SelectIndex(long[] bits) {
            this.bits = bits;
            int cnt = 0;
            for (long w : bits) {
                cnt += Long.bitCount(w);
            }
            ones = cnt;
            int ns = cnt / S + 1;
            sWord = new int[ns];
            sOnes = new int[ns];
            int seen = 0;
            int next = 0;
            for (int w = 0; w < bits.length && next < ns; w++) {
                int pc = Long.bitCount(bits[w]);
                while (next < ns && (long) next * S < seen + pc) {
                    sWord[next] = w;
                    sOnes[next] = seen;
                    next++;
                }
                seen += pc;
            }
            while (next < ns) {
                sWord[next] = bits.length - 1;
                sOnes[next] = cnt;
                next++;
            }
        }

        static int selectInWord(long w, int j) {
            for (; j > 0; j--) {
                w &= w - 1; // clear lowest set bit
            }
            return Long.numberOfTrailingZeros(w);
        }

        int select1(int i) {
            int s = i / S;
            int wi = sWord[s];
            int seen = sOnes[s];
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

        long sizeBytes() {
            return (long) (sWord.length + sOnes.length) * 4;
        }
    }

    // ---- 3. Plain bitmap + sampled select ----
    static final class Bitmap implements Enc {
        final long[] bits;
        final SelectIndex sel;

        Bitmap(int[] a) {
            long u = a[a.length - 1] + 1L;
            bits = new long[(int) ((u + 63) / 64)];
            for (int x : a) {
                bits[x >>> 6] |= 1L << (x & 63);
            }
            sel = new SelectIndex(bits);
        }

        public String name() {
            return "Bitmap+select";
        }

        public long sizeBytes() {
            return (long) bits.length * 8 + sel.sizeBytes();
        }

        public int select(int i) {
            return sel.select1(i);
        }
    }

    // ---- 4. Elias-Fano ----
    static final class EliasFano implements Enc {
        final int l;
        final long[] low;
        final long[] high;
        final SelectIndex sel;

        EliasFano(int[] a) {
            int n = a.length;
            long u = a[n - 1] + 1L;
            l = Math.max(0, (int) Math.floor(Math.log((double) u / n) / Math.log(2)));
            low = new long[(int) (((long) n * l + 63) / 64)];
            long hb = (long) n + (u >>> l) + 1;
            high = new long[(int) ((hb + 63) / 64)];
            long lowMask = (l == 0) ? 0 : ((1L << l) - 1);
            for (int j = 0; j < n; j++) {
                long v = a[j] & 0xFFFFFFFFL;
                if (l > 0) {
                    setBits(low, (long) j * l, l, v & lowMask);
                }
                long p = (v >>> l) + j;
                high[(int) (p >>> 6)] |= 1L << (p & 63);
            }
            sel = new SelectIndex(high);
        }

        public String name() {
            return "Elias-Fano";
        }

        public long sizeBytes() {
            return (long) low.length * 8 + (long) high.length * 8 + sel.sizeBytes();
        }

        public int select(int i) {
            long hp = (long) sel.select1(i) - i;
            long lo = (l == 0) ? 0 : getBits(low, (long) i * l, l);
            return (int) ((hp << l) | lo);
        }
    }

    public static void main(String[] args) {
        int n = args.length > 0 ? Integer.parseInt(args[0]) : 2_097_152;
        double density = args.length > 1 ? Double.parseDouble(args[1]) / 100.0 : 0.98;
        System.out.printf("n=%,d  density=%.2f%n", n, density);

        // sorted, near-dense ids: gap 1 usually, occasional small holes (~simulating merges/frees)
        int[] a = new int[n];
        Random r = new Random(42);
        int cur = 0;
        double holeProb = (1.0 / density) - 1.0;
        for (int i = 0; i < n; i++) {
            a[i] = cur;
            cur += 1 + (r.nextDouble() < holeProb ? 1 + r.nextInt(8) : 0);
        }

        Enc[] encs = { new Baseline(a), new DeltaVByte(a), new ForBitPack(a), new Bitmap(a), new EliasFano(a) };

        Random rc = new Random(7);
        boolean ok = true;
        for (int t = 0; t < 200_000 && ok; t++) {
            int i = rc.nextInt(n);
            int exp = a[i];
            for (Enc e : encs) {
                if (e.select(i) != exp) {
                    System.out.printf("MISMATCH %s i=%d got=%d exp=%d%n", e.name(), i, e.select(i), exp);
                    ok = false;
                    break;
                }
            }
        }
        System.out.println("correctness: " + (ok ? "PASS" : "FAIL"));
        if (!ok) {
            return;
        }

        System.out.printf("%-22s %14s %10s %14s%n", "encoder", "bytes", "bits/elem", "select ns/op");
        int[] idx = new int[1 << 20];
        Random ri = new Random(99);
        for (int i = 0; i < idx.length; i++) {
            idx[i] = ri.nextInt(n);
        }
        for (Enc e : encs) {
            long acc = 0;
            for (int w = 0; w < 3; w++) {
                for (int k : idx) {
                    acc += e.select(k);
                }
            }
            long best = Long.MAX_VALUE;
            for (int it = 0; it < 8; it++) {
                long t0 = System.nanoTime();
                for (int rep = 0; rep < 4; rep++) {
                    for (int k : idx) {
                        acc += e.select(k);
                    }
                }
                best = Math.min(best, System.nanoTime() - t0);
            }
            double nsop = (double) best / ((long) idx.length * 4);
            double bpe = e.sizeBytes() * 8.0 / n;
            System.out.printf("%-22s %,14d %10.2f %14.2f  (acc=%d)%n", e.name(), e.sizeBytes(), bpe, nsop, acc & 1);
        }
    }
}
