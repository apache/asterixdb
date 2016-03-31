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

package org.apache.hyracks.tests.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameWriter;

public class MultiThreadTaskEmulator {

    private ExecutorService executor;

    public MultiThreadTaskEmulator() {
        this.executor = Executors.newCachedThreadPool((r) -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
    }

    public void runInParallel(final IFrameWriter[] writers, final List<IFrame>[] inputFrames) throws Exception {
        final Semaphore sem = new Semaphore(writers.length - 1);
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        for (int i = 1; i < writers.length; i++) {
            sem.acquire();
            final IFrameWriter writer = writers[i];
            final List<IFrame> inputFrame = inputFrames[i];
            executor.execute(() -> {
                executeOneWriter(writer, inputFrame, exceptions);
                sem.release();
            });
        }

        final IFrameWriter writer = writers[0];
        final List<IFrame> inputFrame = inputFrames[0];
        executeOneWriter(writer, inputFrame, exceptions);
        sem.acquire(writers.length - 1);

        for (int i = 0; i < exceptions.size(); i++) {
            exceptions.get(i).printStackTrace();
            if (i == exceptions.size() - 1) {
                throw exceptions.get(i);
            }
        }
    }

    private void executeOneWriter(IFrameWriter writer, List<IFrame> inputFrame, List<Exception> exceptions) {
        try {
            try {
                writer.open();
                for (IFrame frame : inputFrame) {
                    writer.nextFrame(frame.getBuffer());
                }
            } catch (Exception ex) {
                writer.fail();
                throw ex;
            } finally {
                writer.close();
            }
        } catch (Exception e) {
            exceptions.add(e);
        }
    }
}
