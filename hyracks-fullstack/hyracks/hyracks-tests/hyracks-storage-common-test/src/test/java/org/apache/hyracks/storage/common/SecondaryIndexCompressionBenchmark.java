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
package org.apache.hyracks.storage.common;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.storage.common.buffercache.HaltOnFailureCallback;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.buffercache.IFIFOPageWriter;
import org.apache.hyracks.storage.common.buffercache.NoOpPageWriteCallback;
import org.apache.hyracks.storage.common.buffercache.context.write.DefaultBufferCacheWriteContext;
import org.apache.hyracks.storage.common.compression.SnappyCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.compression.file.CompressedFileReference;
import org.apache.hyracks.storage.common.compression.file.ICompressedPageWriter;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.apache.hyracks.test.support.TestStorageManagerComponentHolder;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Measures what block compression costs on the <em>read</em> path of an index, to decide whether
 * secondary indexes should be compressed like the primary already is.
 * <p>
 * The class name deliberately ends in {@code Benchmark}, not {@code Test}, so surefire's
 * {@code **}{@code /*Test.java} include pattern never picks it up in CI. Run it explicitly:
 *
 * <pre>
 * mvn -o -pl hyracks-fullstack/hyracks/hyracks-tests/hyracks-storage-common-test test \
 *     -Dtest=SecondaryIndexCompressionBenchmark
 * </pre>
 *
 * <h2>Two regimes, because they answer opposite halves of the question</h2>
 * In both, the buffer cache is sized to a small fraction of the file, so nearly every pin misses and
 * has to reach the file. What differs is where the bytes come from:
 * <ol>
 * <li><b>Warm OS page cache</b> ({@link #reportRandomPinCost}, {@link #reportSequentialScanCost}) —
 * the file was just written, so no physical device read happens on either side. This isolates the
 * decompression CPU and grants compression none of its benefit, making it the <b>worst case for
 * compression</b>: an overhead that is acceptable here is acceptable anywhere.</li>
 * <li><b>Cold device</b> ({@link #reportColdDeviceCost}) — the page cache is evicted first, so a pin
 * is a real device read. This is the regime that matters for an index larger than memory, and the
 * one where reading fewer bytes can pay for the decompression. Without it the benchmark would only
 * ever report costs and never the corresponding benefit.</li>
 * </ol>
 * Both report <b>microseconds per page access</b>, not just totals, since per-access latency is what
 * a secondary-index probe actually pays.
 * <p>
 * Page content is synthesised to span a range of compressibility rather than to a single guess,
 * because the achieved ratio drives both the space win and the decompression cost. The measured
 * ratios on real data — 36% for a bigint secondary index, 52% for a string one — fall inside the
 * swept range, so the relevant row can be read off directly.
 */
public class SecondaryIndexCompressionBenchmark {

    private static final int PAGE_SIZE = 128 * 1024;
    /** 2048 pages = 64 MiB of logical pages, well beyond the cache below. */
    private static final int FILE_PAGES = 2048;
    /** 64 pages = 2 MiB, so a random pin misses ~97% of the time. */
    private static final int CACHE_PAGES = 64;
    private static final int MAX_OPEN_FILES = 8;
    private static final int RANDOM_PROBES = 20_000;
    private static final int REPEATS = 7;
    private static final int WARMUP_REPEATS = 2;
    private static final int DECOMPRESS_ITERATIONS = 2_000;

    /**
     * Fraction of each page filled with incompressible bytes. Sweeping this sweeps the achieved
     * compression ratio, which is the axis that actually matters.
     */
    private static final double[] INCOMPRESSIBLE_FRACTIONS = { 0.0, 0.10, 0.20, 0.30, 0.50, 0.70, 0.85 };

    /** 32768 pages = 1 GiB logical: big enough that a cold pin is a real device read. */
    private static final int COLD_FILE_PAGES = 32 * 1024;
    private static final int COLD_PROBES = 4_000;
    private static final int COLD_PROBE_SEED = 99;
    /**
     * Calibrated by running the warm sweep and reading the achieved ratio off it: 0.20 lands near the
     * 36% saving measured on a real bigint secondary index. An earlier guess of 0.70 achieved only
     * 12%, which understated both the cost and the benefit.
     */
    private static final double COLD_INCOMPRESSIBLE_FRACTION = 0.20;
    /** GiB of memory pressure applied to evict the page cache; the machine has 32 GiB. */
    private static final int BALLAST_GIB = 22;
    private static final long BALLAST_TIMEOUT_SECONDS = 300;

    private static final List<String> createdFiles = new ArrayList<>();

    private final IHyracksTaskContext ctx = TestUtils.create(PAGE_SIZE);

    private static final class Measurement {
        private final long dataBytes;
        private final long lafBytes;
        private final double medianMillis;

        private Measurement(long dataBytes, long lafBytes, double medianMillis) {
            this.dataBytes = dataBytes;
            this.lafBytes = lafBytes;
            this.medianMillis = medianMillis;
        }

        private long totalBytes() {
            return dataBytes + lafBytes;
        }

        private double microsPerAccess(int accesses) {
            return medianMillis * 1000.0 / accesses;
        }
    }

    /**
     * Attributes the read-path gap. The warm regimes show snappy costing +2 to +22 us per page access,
     * and there are three candidate sources: decompression CPU, the extra buffer-cache pin of the
     * look-aside file that {@code CompressedBufferedFileHandle} needs on every read to locate a page,
     * and the staging copy through the compressed buffer.
     * <p>
     * This measures decompression alone -- same page content, same codec, no buffer cache, no file, no
     * LAF. Whatever the warm delta shows beyond this number is not CPU.
     */
    @Test
    public void reportDecompressionCpuOnly() throws Exception {
        ICompressorDecompressor compDecomp = new SnappyCompressorDecompressorFactory().createInstance();
        StringBuilder out = new StringBuilder();
        out.append("\n=== decompression CPU in isolation (no buffer cache, no file, no LAF) ===\n");
        out.append(String.format("%-14s %13s %8s %14s%n", "incompressible", "compressed", "saved", "decompress us"));

        ByteBuffer uBuffer = ByteBuffer.allocate(PAGE_SIZE);
        ByteBuffer cBuffer = ByteBuffer.allocate(compDecomp.computeCompressedBufferSize(PAGE_SIZE));
        ByteBuffer outBuffer = ByteBuffer.allocate(PAGE_SIZE);
        for (double incompressible : INCOMPRESSIBLE_FRACTIONS) {
            fillLikeIndexLeaf(uBuffer, 1, incompressible);
            uBuffer.position(0).limit(PAGE_SIZE);
            cBuffer.clear();
            compDecomp.compress(uBuffer, cBuffer);
            int compressedSize = cBuffer.limit();

            double[] millis = new double[REPEATS];
            for (int repeat = 0; repeat < REPEATS; repeat++) {
                long start = System.nanoTime();
                for (int i = 0; i < DECOMPRESS_ITERATIONS; i++) {
                    cBuffer.position(0).limit(compressedSize);
                    outBuffer.clear();
                    compDecomp.uncompress(cBuffer, outBuffer);
                }
                millis[repeat] = (System.nanoTime() - start) / 1_000_000.0;
            }
            double us = medianAfterWarmup(millis) * 1000.0 / DECOMPRESS_ITERATIONS;
            out.append(String.format("%13.0f%% %13d %7.1f%% %14.2f%n", incompressible * 100, compressedSize,
                    (1.0 - (double) compressedSize / PAGE_SIZE) * 100, us));
        }
        System.out.println(out); // NOSONAR
    }

    @Test
    public void reportRandomPinCost() throws Exception {
        report("random pin (" + RANDOM_PROBES + " scattered pins)", false);
    }

    @Test
    public void reportSequentialScanCost() throws Exception {
        report("sequential scan (" + FILE_PAGES + " pages in order)", true);
    }

    private void report(String label, boolean sequential) throws Exception {
        int accesses = sequential ? FILE_PAGES : RANDOM_PROBES;
        StringBuilder out = new StringBuilder();
        out.append("\n=== secondary index compression, ").append(label).append(" (OS page cache WARM) ===\n");
        out.append(String.format("%-14s %13s %13s %7s %10s %10s %10s %10s%n", "incompressible", "none bytes",
                "snappy bytes", "saved", "none us", "snappy us", "delta us", "overhead"));

        for (double incompressible : INCOMPRESSIBLE_FRACTIONS) {
            Measurement plain = measure(false, incompressible, sequential);
            Measurement snappy = measure(true, incompressible, sequential);

            double saved = 1.0 - (double) snappy.totalBytes() / plain.totalBytes();
            double plainUs = plain.microsPerAccess(accesses);
            double snappyUs = snappy.microsPerAccess(accesses);
            out.append(String.format("%13.0f%% %13d %13d %6.1f%% %10.2f %10.2f %10.2f %9.1f%%%n", incompressible * 100,
                    plain.totalBytes(), snappy.totalBytes(), saved * 100, plainUs, snappyUs, snappyUs - plainUs,
                    (snappyUs / plainUs - 1.0) * 100));
        }
        // A benchmark's whole output is its result, so print it rather than log it.
        System.out.println(out); // NOSONAR
    }

    /**
     * The case the warm sweep cannot reach: the index does not fit in memory, so a pin costs a real
     * device read. Here compression trades decompression CPU for fewer bytes off the device, which is
     * the trade that decides whether secondary indexes should be compressed.
     * <p>
     * A file larger than RAM is not reachable on this class of machine (32 GiB RAM against 33 GiB of
     * free disk), and {@code purge} needs root. So instead the file stays moderate and the OS page
     * cache is evicted by putting the machine under memory pressure — see
     * {@link #evictOsPageCache()}. Each side is then read once cold and once warm; the cold/warm gap
     * is the evidence that eviction actually happened, and is printed so a run where it did not is
     * self-evident rather than silently reported as a cold number.
     */
    @Test
    public void reportColdDeviceCost() throws Exception {
        if (!canEvictOsPageCache()) {
            System.out.println("\n=== cold-device: SKIPPED, no python3 to apply memory pressure ===\n"); // NOSONAR
            return;
        }
        StringBuilder out = new StringBuilder();
        out.append("\n=== secondary index compression, cold device (OS page cache EVICTED) ===\n");
        out.append(String.format("file %d MiB logical, buffer cache %d MiB%n",
                (long) COLD_FILE_PAGES * PAGE_SIZE / (1024 * 1024), (long) CACHE_PAGES * PAGE_SIZE / (1024 * 1024)));
        out.append(String.format("%-12s %-8s %13s %7s %10s %10s %10s %12s%n", "pattern", "scheme", "bytes", "saved",
                "cold us", "warm us", "cold/warm", "cold MB/s"));

        // Random probes are latency-bound and sequential scans are bandwidth-bound, and compression
        // only helps the second. Reporting one without the other would answer half the question: an
        // ordinary secondary index is probed, but the sample index is fully scanned by the CBO.
        for (boolean sequential : new boolean[] { false, true }) {
            int accesses = sequential ? COLD_FILE_PAGES : COLD_PROBES;
            long plainBytes = -1;
            for (boolean compressed : new boolean[] { false, true }) {
                ColdMeasurement m = measureCold(compressed, sequential);
                if (!compressed) {
                    plainBytes = m.totalBytes;
                }
                double coldUs = m.coldMillis * 1000.0 / accesses;
                double warmUs = m.warmMillis * 1000.0 / accesses;
                double coldMbPerSec = (double) accesses * PAGE_SIZE / (1024 * 1024) / (m.coldMillis / 1000.0);
                double saved = 1.0 - (double) m.totalBytes / plainBytes;
                out.append(String.format("%-12s %-8s %13d %6.1f%% %10.2f %10.2f %10.1fx %12.1f%n",
                        sequential ? "sequential" : "random", compressed ? "snappy" : "none", m.totalBytes, saved * 100,
                        coldUs, warmUs, coldUs / warmUs, coldMbPerSec));
            }
        }
        System.out.println(out); // NOSONAR
    }

    private static final class ColdMeasurement {
        private final long totalBytes;
        private final double coldMillis;
        private final double warmMillis;

        private ColdMeasurement(long totalBytes, double coldMillis, double warmMillis) {
            this.totalBytes = totalBytes;
            this.coldMillis = coldMillis;
            this.warmMillis = warmMillis;
        }
    }

    private ColdMeasurement measureCold(boolean compressed, boolean sequential) throws Exception {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, CACHE_PAGES, MAX_OPEN_FILES);
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        FileReference fileRef = newFileReference(ioManager, compressed, COLD_INCOMPRESSIBLE_FRACTION, sequential);
        try {
            int fileId = bufferCache.createFile(fileRef);
            writePages(bufferCache, fileId, COLD_INCOMPRESSIBLE_FRACTION, COLD_FILE_PAGES);
            long totalBytes = ioManager.getSize(fileRef)
                    + (compressed ? ioManager.getSize(((CompressedFileReference) fileRef).getLAFFileReference()) : 0L);

            evictOsPageCache();

            bufferCache.openFile(fileId);
            // Identical access sequence in both passes, so cold and warm differ only in where the
            // bytes came from.
            long start = System.nanoTime();
            coldAccess(bufferCache, fileId, sequential);
            double coldMillis = (System.nanoTime() - start) / 1_000_000.0;

            start = System.nanoTime();
            coldAccess(bufferCache, fileId, sequential);
            double warmMillis = (System.nanoTime() - start) / 1_000_000.0;

            bufferCache.closeFile(fileId);
            bufferCache.deleteFile(fileId);
            return new ColdMeasurement(totalBytes, coldMillis, warmMillis);
        } finally {
            bufferCache.close();
        }
    }

    private static void coldAccess(IBufferCache bufferCache, int fileId, boolean sequential)
            throws HyracksDataException {
        if (sequential) {
            scanPages(bufferCache, fileId, COLD_FILE_PAGES);
        } else {
            pinRandomPages(bufferCache, fileId, COLD_PROBE_SEED, COLD_PROBES, COLD_FILE_PAGES);
        }
    }

    private static boolean canEvictOsPageCache() {
        try {
            return new ProcessBuilder("python3", "-c", "pass").start().waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Evicts file-backed pages by allocating and touching most of RAM in a short-lived subprocess.
     * Clean file pages are the first thing the OS gives up under this pressure, so the benchmark file
     * leaves the page cache. A subprocess is used because the surefire {@code argLine} in the parent
     * pom pins this JVM to {@code -Xmx2048m}, and it cannot be overridden from the command line.
     */
    private static void evictOsPageCache() throws Exception {
        // The ballast must be INCOMPRESSIBLE. macOS compresses anonymous memory before it gives up
        // file-backed pages, and zero-filled ballast compresses to nearly nothing -- the page cache
        // would survive and every "cold" number would silently be a warm one. So every page is filled
        // from a random block: the compressor works per page, so identical random pages are still
        // individually incompressible, which lets one 16 MiB urandom block seed all of it cheaply.
        String script = "import os\n" + "block = os.urandom(1 << 24)\n" + "held = []\n" + "for _ in range("
                + BALLAST_GIB + "):\n" + "    held.append(bytearray(block * 64))\n" + "del held\n";
        Process p = new ProcessBuilder("python3", "-c", script).redirectErrorStream(true).start();
        if (!p.waitFor(BALLAST_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            p.destroyForcibly();
        }
    }

    private Measurement measure(boolean compressed, double incompressibleFraction, boolean sequential)
            throws Exception {
        TestStorageManagerComponentHolder.init(PAGE_SIZE, CACHE_PAGES, MAX_OPEN_FILES);
        IIOManager ioManager = TestStorageManagerComponentHolder.getIOManager();
        IBufferCache bufferCache =
                TestStorageManagerComponentHolder.getBufferCache(ctx.getJobletContext().getServiceContext());
        FileReference fileRef = newFileReference(ioManager, compressed, incompressibleFraction, sequential);
        try {
            int fileId = bufferCache.createFile(fileRef);
            writePages(bufferCache, fileId, incompressibleFraction, FILE_PAGES);

            long dataBytes = ioManager.getSize(fileRef);
            long lafBytes =
                    compressed ? ioManager.getSize(((CompressedFileReference) fileRef).getLAFFileReference()) : 0L;

            bufferCache.openFile(fileId);
            double[] millis = new double[REPEATS];
            for (int repeat = 0; repeat < REPEATS; repeat++) {
                long start = System.nanoTime();
                if (sequential) {
                    scanPages(bufferCache, fileId, FILE_PAGES);
                } else {
                    pinRandomPages(bufferCache, fileId, repeat, RANDOM_PROBES, FILE_PAGES);
                }
                millis[repeat] = (System.nanoTime() - start) / 1_000_000.0;
            }
            bufferCache.closeFile(fileId);
            bufferCache.deleteFile(fileId);
            return new Measurement(dataBytes, lafBytes, medianAfterWarmup(millis));
        } finally {
            bufferCache.close();
        }
    }

    private void writePages(IBufferCache bufferCache, int fileId, double incompressibleFraction, int filePages)
            throws HyracksDataException {
        bufferCache.openFile(fileId);
        ICompressedPageWriter compressedPageWriter = bufferCache.getCompressedPageWriter(fileId);
        IFIFOPageWriter pageWriter = bufferCache.createFIFOWriter(NoOpPageWriteCallback.INSTANCE,
                HaltOnFailureCallback.INSTANCE, DefaultBufferCacheWriteContext.INSTANCE);
        for (int pageId = 0; pageId < filePages; pageId++) {
            long dpid = BufferedFileHandle.getDiskPageId(fileId, pageId);
            ICachedPage page = bufferCache.confiscatePage(dpid);
            compressedPageWriter.prepareWrite(page);
            fillLikeIndexLeaf(page.getBuffer(), pageId, incompressibleFraction);
            pageWriter.write(page);
        }
        compressedPageWriter.endWriting();
        bufferCache.closeFile(fileId);
    }

    /**
     * Writes something shaped like a btree leaf of a secondary index: a run of ascending keys,
     * which compress well because their high-order bytes repeat, followed by a block of
     * incompressible bytes standing in for high-entropy primary keys. The split between the two is
     * what {@code incompressibleFraction} controls.
     * <p>
     * Content is seeded from the page id alone, so the compressed and uncompressed runs of a given
     * configuration see byte-identical input.
     */
    private static void fillLikeIndexLeaf(ByteBuffer buf, int pageId, double incompressibleFraction) {
        Random rnd = new Random(pageId);
        int incompressibleBytes = (int) (PAGE_SIZE * incompressibleFraction);
        int structuredBytes = PAGE_SIZE - incompressibleBytes;

        buf.position(0);
        long key = (long) pageId * 100_000L;
        int written = 0;
        while (written + Long.BYTES <= structuredBytes) {
            buf.putLong(key);
            key += 3;
            written += Long.BYTES;
        }
        byte[] noise = new byte[PAGE_SIZE - written];
        rnd.nextBytes(noise);
        buf.put(noise);
        buf.position(0);
    }

    private static void pinRandomPages(IBufferCache bufferCache, int fileId, int seed, int probes, int filePages)
            throws HyracksDataException {
        Random rnd = new Random(seed);
        for (int probe = 0; probe < probes; probe++) {
            long dpid = BufferedFileHandle.getDiskPageId(fileId, rnd.nextInt(filePages));
            ICachedPage page = bufferCache.pin(dpid);
            bufferCache.unpin(page);
        }
    }

    private static void scanPages(IBufferCache bufferCache, int fileId, int filePages) throws HyracksDataException {
        for (int pageId = 0; pageId < filePages; pageId++) {
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
            bufferCache.unpin(page);
        }
    }

    /** Drops the first repeats as JIT warm-up, then takes the median so a single stall cannot skew it. */
    private static double medianAfterWarmup(double[] millis) {
        double[] measured = Arrays.copyOfRange(millis, WARMUP_REPEATS, millis.length);
        Arrays.sort(measured);
        int mid = measured.length / 2;
        return measured.length % 2 == 1 ? measured[mid] : (measured[mid - 1] + measured[mid]) / 2;
    }

    private static FileReference newFileReference(IIOManager ioManager, boolean compressed,
            double incompressibleFraction, boolean sequential) throws HyracksDataException {
        String name = String.format("sicb-%s-%s-%02d", sequential ? "scan" : "rand", compressed ? "snappy" : "none",
                (int) (incompressibleFraction * 100));
        FileReference fileRef = ioManager.resolve(name);
        // A run that died before its cleanup would otherwise leave a file that fails createFile with
        // HYR0082 forever, so start from a clean slate rather than depending on the previous run.
        deleteIfExists(ioManager, fileRef);
        createdFiles.add(name);
        if (!compressed) {
            return fileRef;
        }
        ICompressorDecompressor compDecomp = new SnappyCompressorDecompressorFactory().createInstance();
        createdFiles.add(name + ".dic");
        CompressedFileReference cFileRef = new CompressedFileReference(fileRef.getDeviceHandle(), compDecomp,
                fileRef.getRelativePath(), fileRef.getRelativePath() + ".dic");
        deleteIfExists(ioManager, cFileRef.getLAFFileReference());
        return cFileRef;
    }

    private static void deleteIfExists(IIOManager ioManager, FileReference fileRef) throws HyracksDataException {
        if (ioManager.exists(fileRef)) {
            ioManager.delete(fileRef);
        }
    }

    @AfterClass
    public static void cleanup() {
        createdFiles.clear();
    }
}
