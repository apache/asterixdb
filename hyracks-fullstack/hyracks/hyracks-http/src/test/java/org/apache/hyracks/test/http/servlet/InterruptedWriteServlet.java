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
package org.apache.hyracks.test.http.servlet;

import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Streams rows to a client that does not read them, so that the channel stops accepting and the writing thread waits
 * for it; a cancel then interrupts that wait and the write it was part of is lost. What a
 * {@link PrintWriter} writes after that - it records the failure and carries on - must not reach the client, and the
 * response must not be terminated as a complete one.
 */
public class InterruptedWriteServlet extends AbstractServlet {

    /** What the servlet writes once its writer has failed; the client must never see it. */
    public static final String AFTER_THE_FAILURE = "the-write-that-must-not-land";
    /** A bound on the rows written, so a channel that never fills cannot hang the test. */
    private static final int MAX_ROWS = 8192;
    private static final String ROW = "0123456789abcdef".repeat(64);

    private final CountDownLatch writing = new CountDownLatch(1);
    private final CountDownLatch finished = new CountDownLatch(1);
    private volatile Thread writer;
    private volatile boolean errorObserved;

    public InterruptedWriteServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        response.setStatus(HttpResponseStatus.OK);
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, request);
        writer = Thread.currentThread();
        PrintWriter out = response.writer();
        try {
            writing.countDown();
            // checkError() flushes, so each row reaches the channel and the loop ends at the first failed write
            for (int row = 0; row < MAX_ROWS && !out.checkError(); row++) {
                out.print(ROW);
            }
            errorObserved = out.checkError();
            out.print(AFTER_THE_FAILURE);
            out.flush();
        } finally {
            // the wait that was interrupted left the flag set; clear it before the thread goes back to the pool
            Thread.interrupted();
            finished.countDown();
        }
    }

    /** Interrupts the writing thread once it has started, as a cancel of the request does. */
    public void interruptWriter(long timeout, TimeUnit unit) throws InterruptedException {
        if (!writing.await(timeout, unit)) {
            throw new IllegalStateException("the servlet never started writing");
        }
        writer.interrupt();
    }

    public boolean awaitFinished(long timeout, TimeUnit unit) throws InterruptedException {
        return finished.await(timeout, unit);
    }

    /** Whether the writer did fail, which is what the test needs to have raced for. */
    public boolean errorObserved() {
        return errorObserved;
    }
}
