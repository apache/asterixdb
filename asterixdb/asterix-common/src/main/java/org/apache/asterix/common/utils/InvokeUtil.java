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
package org.apache.asterix.common.utils;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InvokeUtil {

    private static final Logger LOGGER = Logger.getLogger(InvokeUtil.class.getName());

    /**
     * Executes the passed interruptible, retrying if the operation is interrupted. Once the interruptible
     * completes, the current thread will be re-interrupted, if the original operation was interrupted.
     */
    public static void doUninterruptibly(Interruptible interruptible) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    interruptible.run();
                    break;
                } catch (InterruptedException e) { // NOSONAR- we will re-interrupt the thread during unwind
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Executes the passed interruptible, retrying if the operation is interrupted. Once the interruptible
     * completes, the current thread will be re-interrupted, if the original operation was interrupted.
     */
    public static void doExUninterruptibly(ThrowingInterruptible interruptible) throws Exception {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    interruptible.run();
                    break;
                } catch (InterruptedException e) { // NOSONAR- we will re-interrupt the thread during unwind
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Executes the passed interruptible, retrying if the operation is interrupted.
     *
     * @return true if the original operation was interrupted, otherwise false
     */
    public static boolean doUninterruptiblyGet(Interruptible interruptible) {
        boolean interrupted = false;
        while (true) {
            try {
                interruptible.run();
                break;
            } catch (InterruptedException e) { // NOSONAR- contract states caller must handle
                interrupted = true;
            }
        }
        return interrupted;
    }

    /**
     * Executes the passed interruptible, retrying if the operation is interrupted. If the operation throws an
     * exception after being previously interrupted, the current thread will be re-interrupted.
     *
     * @return true if the original operation was interrupted, otherwise false
     */
    public static boolean doExUninterruptiblyGet(ThrowingInterruptible interruptible) throws Exception {
        boolean interrupted = false;
        boolean success = false;
        while (true) {
            try {
                interruptible.run();
                success = true;
                break;
            } catch (InterruptedException e) { // NOSONAR- contract states caller must handle
                interrupted = true;
            } finally {
                if (!success && interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return interrupted;
    }

    public static boolean retryLoop(long duration, TimeUnit durationUnit, long delay, TimeUnit delayUnit,
            Callable<Boolean> function) throws IOException {
        long endTime = System.nanoTime() + durationUnit.toNanos(duration);
        boolean first = true;
        while (endTime - System.nanoTime() > 0) {
            if (first) {
                first = false;
            } else {
                try {
                    delayUnit.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            try {
                if (function.call()) {
                    return true;
                }
            } catch (Exception e) {
                // ignore, retry after delay
                LOGGER.log(Level.FINE, "Ignoring exception on retryLoop attempt, will retry after delay", e);
            }
        }
        return false;
    }

    @FunctionalInterface
    public interface Interruptible {
        void run() throws InterruptedException;
    }

    @FunctionalInterface
    public interface ThrowingInterruptible {
        void run() throws Exception; // NOSONAR
    }

}
