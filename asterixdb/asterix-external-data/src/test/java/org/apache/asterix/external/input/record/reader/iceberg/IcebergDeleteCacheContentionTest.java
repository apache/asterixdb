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
package org.apache.asterix.external.input.record.reader.iceberg;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

/**
 * {@link IcebergDeleteCache}'s budget reservation under contention: concurrent misses on the same and on different
 * delete files, failing loads, and the exact budget boundary. These are unit tests because the defects only show when
 * readers race, and in the cache's own size accounting, which no query can observe.
 */
public class IcebergDeleteCacheContentionTest {

    private static final long ENTRY = 10;
    private static final int READERS = 16;

    private static final Method GET_OR_LOAD;
    private static final Field ENTRIES;

    static {
        try {
            GET_OR_LOAD =
                    IcebergDeleteCache.class.getDeclaredMethod("getOrLoad", String.class, Supplier.class, long.class);
            GET_OR_LOAD.setAccessible(true);
            ENTRIES = IcebergDeleteCache.class.getDeclaredField("entries");
            ENTRIES.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static Object getOrLoad(IcebergDeleteCache cache, String key, Supplier<Object> supplier, long size)
            throws Throwable {
        try {
            return GET_OR_LOAD.invoke(cache, key, supplier, size);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    /** The cache's entries, each as its load task. */
    private static Map<String, FutureTask<Object>> entries(IcebergDeleteCache cache) throws Exception {
        Map<String, FutureTask<Object>> tasks = new java.util.HashMap<>();
        for (Map.Entry<?, ?> e : ((Map<?, ?>) ENTRIES.get(cache)).entrySet()) {
            Object value = e.getValue();
            if (!(value instanceof FutureTask)) {
                // an entry holder wrapping its load task
                Field task = value.getClass().getDeclaredField("task");
                task.setAccessible(true);
                value = task.get(value);
            }
            @SuppressWarnings("unchecked")
            FutureTask<Object> load = (FutureTask<Object>) value;
            tasks.put((String) e.getKey(), load);
        }
        return tasks;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Many readers miss on the SAME delete file at once, which is how a scan starts: every partition reader needs the
     * same equality delete. Claim: it is loaded once, and nobody holds a second copy. The budget fits 3 entries, so
     * transient reservations by the 16 readers can exhaust it while they race to insert.
     */
    @Test(timeout = 120_000)
    public void sameKeyIsLoadedOnce() throws Exception {
        int rounds = 300;
        int roundsWithExtraLoads = 0;
        for (int round = 0; round < rounds; round++) {
            IcebergDeleteCache cache = new IcebergDeleteCache(ENTRY, 3 * ENTRY);
            AtomicInteger loads = new AtomicInteger();
            runConcurrently(READERS, r -> getOrLoad(cache, "delete-file", () -> {
                loads.incrementAndGet();
                sleep(5);
                return "rows";
            }, ENTRY));
            if (loads.get() > 1) {
                roundsWithExtraLoads++;
            }
        }
        Assert.assertEquals("rounds in which the same delete file was loaded more than once", 0, roundsWithExtraLoads);
    }

    /**
     * 15 readers miss on file K1 while one reader misses on K2; the budget fits both. Claim: K2 is cached. Transient
     * reservations by K1's losers can make K2's reservation fail although K2 fits once they hand back.
     */
    @Test(timeout = 120_000)
    public void otherKeyIsNotRefusedByTransientReservations() throws Exception {
        int rounds = 300;
        int refused = 0;
        for (int round = 0; round < rounds; round++) {
            IcebergDeleteCache cache = new IcebergDeleteCache(ENTRY, 2 * ENTRY);
            runConcurrently(READERS, r -> getOrLoad(cache, r == 0 ? "K2" : "K1", () -> {
                sleep(5);
                return "rows";
            }, ENTRY));
            if (!entries(cache).containsKey("K2") || !entries(cache).containsKey("K1")) {
                refused++;
            }
        }
        Assert.assertEquals("rounds where a file that fits the budget was loaded uncached", 0, refused);
    }

    /** The exact boundary: entries summing to exactly the budget are admitted; one byte more is not. */
    @Test(timeout = 30_000)
    public void exactBoundaryIsAdmitted() throws Throwable {
        IcebergDeleteCache cache = new IcebergDeleteCache(ENTRY, 3 * ENTRY);
        for (int i = 0; i < 3; i++) {
            getOrLoad(cache, "f" + i, () -> "rows", ENTRY);
        }
        long atBoundary = cache.cachedSize();
        int entriesAtBoundary = cache.size();
        getOrLoad(cache, "f3", () -> "rows", 1);
        long after = cache.cachedSize();
        // concurrent: 16 distinct files, room for exactly 3
        int underFilled = 0;
        int overFilled = 0;
        for (int round = 0; round < 200; round++) {
            IcebergDeleteCache c = new IcebergDeleteCache(ENTRY, 3 * ENTRY);
            runConcurrently(READERS, r -> getOrLoad(c, "d" + r, () -> {
                sleep(2);
                return "rows";
            }, ENTRY));
            underFilled += c.size() < 3 ? 1 : 0;
            overFilled += c.size() > 3 ? 1 : 0;
        }
        Assert.assertEquals(3, entriesAtBoundary);
        Assert.assertEquals(3 * ENTRY, atBoundary);
        Assert.assertEquals(3 * ENTRY, after);
        Assert.assertEquals(0, overFilled);
        Assert.assertEquals(0, underFilled);
    }

    /**
     * One cache, many rounds of mixed traffic: 16 readers, 40 files of different sizes, a third of first loads fail,
     * same and different keys mixed. A sampler checks the budget all along. After everything settles, cachedSize must
     * equal the sum of the live entries' sizes, and no failed entry may be left behind.
     */
    @Test(timeout = 120_000)
    public void accountingDoesNotDriftUnderFailures() throws Exception {
        long budget = 200;
        IcebergDeleteCache cache = new IcebergDeleteCache(64, budget);
        int files = 40;
        long[] sizes = new long[files];
        for (int i = 0; i < files; i++) {
            sizes[i] = 5 + (i * 7) % 23;
        }
        AtomicInteger[] attempts = new AtomicInteger[files];
        for (int i = 0; i < files; i++) {
            attempts[i] = new AtomicInteger();
        }
        AtomicBoolean done = new AtomicBoolean();
        AtomicLong maxSeen = new AtomicLong();
        Thread sampler = new Thread(() -> {
            while (!done.get()) {
                maxSeen.accumulateAndGet(cache.cachedSize(), Math::max);
            }
        });
        sampler.start();
        int opsPerReader = 3000;
        runConcurrently(READERS, r -> {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();
            for (int op = 0; op < opsPerReader; op++) {
                int f = rnd.nextInt(10) < 5 ? rnd.nextInt(4) : rnd.nextInt(files);
                try {
                    getOrLoad(cache, "file-" + f, () -> {
                        int n = attempts[f].incrementAndGet();
                        if (n % 3 == 1) {
                            throw new IllegalStateException("load failure " + n);
                        }
                        return "rows-" + f;
                    }, sizes[f]);
                } catch (IllegalStateException expected) {
                    // every third load of a file fails, and the next reader retries it
                }
            }
            return null;
        });
        done.set(true);
        sampler.join();
        long sum = 0;
        int failedLeft = 0;
        for (Map.Entry<String, FutureTask<Object>> e : entries(cache).entrySet()) {
            try {
                e.getValue().get();
                sum += sizes[Integer.parseInt(e.getKey().substring("file-".length()))];
            } catch (Exception ex) {
                failedLeft++;
            }
        }
        Assert.assertEquals("cachedSize vs the sum of live entries", sum, cache.cachedSize());
        Assert.assertEquals("failed entries left in the cache", 0, failedLeft);
        Assert.assertTrue("cachedSize reached " + maxSeen.get() + " over a budget of " + budget,
                maxSeen.get() <= budget);
    }

    /** Loads of different files must still overlap: 8 files, 200 ms each, 8 readers. */
    @Test(timeout = 30_000)
    public void loadsOfDifferentKeysOverlap() throws Exception {
        IcebergDeleteCache cache = new IcebergDeleteCache(ENTRY, 100 * ENTRY);
        long t0 = System.nanoTime();
        runConcurrently(8, r -> getOrLoad(cache, "k" + r, () -> {
            sleep(200);
            return "rows";
        }, ENTRY));
        long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
        Assert.assertTrue("8 loads of 200 ms took " + ms + " ms", ms < 800);
    }

    interface Body {
        Object run(int reader) throws Throwable;
    }

    private static void runConcurrently(int readers, Body body) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        List<Future<?>> futures = new ArrayList<>();
        for (int r = 0; r < readers; r++) {
            int rr = r;
            futures.add(pool.submit(() -> {
                start.await();
                try {
                    return body.run(rr);
                } catch (Throwable t) {
                    throw new RuntimeException(t);
                }
            }));
        }
        start.countDown();
        try {
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Readers missing on different delete files at the same moment used to each pass the budget check before any of
     * them added its entry, so the cache held many times its budget.
     */
    @Test(timeout = 120_000)
    public void concurrentMissesStayWithinTheTotalBudget() throws Exception {
        long worst = 0;
        for (int round = 0; round < 50; round++) {
            IcebergDeleteCache cache = new IcebergDeleteCache(ENTRY, ENTRY);
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(READERS);
            try {
                List<Future<Object>> futures = new ArrayList<>();
                for (int r = 0; r < READERS; r++) {
                    String key = "delete-file-" + r;
                    futures.add(pool.submit(() -> {
                        start.await();
                        return GET_OR_LOAD.invoke(cache, key, (Supplier<Object>) () -> {
                            sleep(5);
                            return key;
                        }, ENTRY);
                    }));
                }
                start.countDown();
                for (Future<Object> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }
            worst = Math.max(worst, cache.cachedSize());
        }
        Assert.assertTrue("cache held " + worst + " against a total budget of " + ENTRY, worst <= ENTRY);
    }

    /**
     * Readers missing on the same delete file at once, with room for exactly that one entry, must all wait for one
     * load: a claim taken by every racer before it looked the file up made the others' claims fail and sent them to
     * load their own copy.
     */
    @Test(timeout = 60_000)
    public void sameFileRaceAtAFullBudgetLoadsOnce() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(READERS);
        int worstLoads = 0;
        try {
            for (int round = 0; round < 200; round++) {
                IcebergDeleteCache cache = new IcebergDeleteCache(100, 100);
                AtomicInteger loads = new AtomicInteger();
                CountDownLatch start = new CountDownLatch(1);
                List<Future<Object>> results = new ArrayList<>();
                for (int r = 0; r < READERS; r++) {
                    results.add(pool.submit(() -> {
                        start.await();
                        return GET_OR_LOAD.invoke(cache, "same", (Supplier<Object>) () -> {
                            loads.incrementAndGet();
                            sleep(2);
                            return "shared";
                        }, 100L);
                    }));
                }
                start.countDown();
                for (Future<Object> f : results) {
                    Assert.assertEquals("round " + round, "shared", f.get(30, TimeUnit.SECONDS));
                }
                Assert.assertEquals("round " + round + " cachedSize", 100, cache.cachedSize());
                worstLoads = Math.max(worstLoads, loads.get());
            }
        } finally {
            pool.shutdownNow();
        }
        Assert.assertEquals("most loads of the one file in a round", 1, worstLoads);
    }
}
