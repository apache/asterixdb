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
package org.apache.asterix.test.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/**
 * Covers the thread leak check {@link LangExecutionUtil#checkThreadLeaks} runs at the end of every runtime suite.
 * <p>
 * The check used to search the entire thread dump -- names <em>and</em> stack frames -- for {@code Operator},
 * {@code SuperActivity} and {@code PipelinedPartition}. Stack frames are not evidence of a leak: reactor-netty's
 * {@code InternalMonoOperator} appears in the stack of the Azure client's event loop, a thread that lives for the
 * whole JVM, so an azblob-backed suite failed whenever that loop happened to be inside a subscribe() at dump time.
 * Only the thread's name identifies it as the execution engine's, which is what these tests pin.
 */
public class LangExecutionUtilTest {

    /** A stand-in for reactor's {@code InternalMonoOperator}: only its class name carries the marker. */
    private static final class InternalMonoOperatorLookalike {
        private static void park(CountDownLatch parked, CountDownLatch release) throws InterruptedException {
            parked.countDown();
            release.await();
        }
    }

    @Test
    public void aThreadNamedAfterAnOperatorIsALeak() throws Exception {
        // Task names its threads "<operator display name>:<jobId>:<taskAttemptId>:<index>"
        assertReported("SortOperatorNodePushable@1b2c3d:JID:0:TAID:0:0", true);
    }

    @Test
    public void aMaterializingPipelinedPartitionThreadIsALeak() throws Exception {
        assertReported("MaterializingPipelinedPartition PID:0:0", true);
    }

    /** {@code SuperActivityOperatorNodePushable.getDisplayName()} returns "Super Activity [...]", with a space. */
    @Test
    public void aSuperActivityThreadIsALeak() throws Exception {
        assertReported("Super Activity [ANID:0]:JID:0:TAID:0:0", true);
    }

    /**
     * The regression: a thread the engine does not own, whose stack merely mentions a class with {@code Operator}
     * in its name, is not a leak.
     */
    @Test
    public void anOperatorFrameOnAnUnrelatedThreadIsNotALeak() throws Exception {
        assertReported("reactor-http-nio-2", false);
    }

    /**
     * Runs a thread with the given name, parked inside {@link InternalMonoOperatorLookalike} so that every case
     * has an {@code Operator} frame on its stack, and asserts whether the check reports that thread. Only this
     * thread is asserted on -- surefire reuses a fork across classes, so the reported list is not ours alone.
     */
    private static void assertReported(String threadName, boolean expectReported) throws Exception {
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                InternalMonoOperatorLookalike.park(parked, release);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, threadName);
        thread.setDaemon(true);
        thread.start();
        try {
            assertTrue("thread did not park", parked.await(30, TimeUnit.SECONDS));
            List<String> leaked = leakedExecutionEngineThreads();
            assertEquals("reported leaks: " + leaked, expectReported, leaked.contains(threadName));
        } finally {
            release.countDown();
            thread.join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> leakedExecutionEngineThreads() throws Exception {
        Method method = LangExecutionUtil.class.getDeclaredMethod("leakedExecutionEngineThreads");
        method.setAccessible(true);
        return (List<String>) method.invoke(null);
    }
}
