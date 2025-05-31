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
package org.apache.hyracks.api.util;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.ComputingAction;
import org.apache.hyracks.util.IDelay;
import org.apache.hyracks.util.IOInterruptibleAction;
import org.apache.hyracks.util.IOThrowingAction;
import org.apache.hyracks.util.IRetryPolicy;
import org.apache.hyracks.util.InterruptibleAction;
import org.apache.hyracks.util.InterruptibleSupplier;
import org.apache.hyracks.util.Span;
import org.apache.hyracks.util.ThrowingAction;
import org.apache.hyracks.util.ThrowingConsumer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.UncheckedExecutionException;

public class InvokeUtil {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final IFailedAttemptCallback defaultFailureCallback =
            (action, attempt, isFinal, span, failure) -> LOGGER.log(Level.WARN,
                    "failure executing action {} (attempt: {}{})", action, attempt, isFinal ? "" : ", will retry",
                    failure);

    private InvokeUtil() {
    }

    /**
     * Executes the passed interruptible, retrying if the operation is interrupted. Once the interruptible
     * completes, the current thread will be re-interrupted, if the original operation was interrupted.
     *
     * @deprecated this method does not throw an exception when the action is interrupted, or when the thread
     *             was interrupted prior to calling this method. This can lead to confusing behavior, if the caller
     *             does not check for interrupted thread state after calling this method.
     */
    @Deprecated
    public static void doUninterruptibly(InterruptibleAction interruptible) {
        boolean interrupted = Thread.interrupted();
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
     * Executes the passed action, retrying if the operation is interrupted. Once the interruptible
     * completes, the current thread will be re-interrupted, if the original operation was interrupted.
     */
    public static void doExUninterruptibly(ThrowingAction interruptible) throws Exception {
        boolean interrupted = Thread.interrupted();
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
     * Executes the passed interruptible supplier, retrying if the operation is interrupted. Once the interruptible
     * supplier completes, the current thread will be re-interrupted, if the original operation was interrupted.
     */
    public static <T> T getUninterruptibly(InterruptibleSupplier<T> interruptible) {
        boolean interrupted = Thread.interrupted();
        try {
            while (true) {
                try {
                    return interruptible.get();
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
    public static boolean doUninterruptiblyGet(InterruptibleAction interruptible) {
        boolean interrupted = Thread.interrupted();
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
    public static boolean doExUninterruptiblyGet(Callable<Void> interruptible) throws Exception {
        boolean interrupted = Thread.interrupted();
        boolean success = false;
        while (true) {
            try {
                interruptible.call();
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
                LOGGER.log(Level.DEBUG, "Ignoring exception on retryLoop attempt, will retry after delay", e);
            }
        }
        return false;
    }

    /**
     * Executes the passed interruptible, retrying if the operation fails due to {@link ClosedByInterruptException} or
     * {@link InterruptedException}. Once the interruptible completes, the current thread will be re-interrupted, if
     * the original operation was interrupted.
     */
    public static void doIoUninterruptibly(IOInterruptibleAction interruptible) throws IOException {
        boolean interrupted = Thread.interrupted();
        try {
            while (true) {
                try {
                    interruptible.run();
                    break;
                } catch (ClosedByInterruptException | InterruptedException e) {
                    LOGGER.error("IO operation Interrupted. Retrying..", e);
                    interrupted = true;
                    //noinspection ResultOfMethodCallIgnored
                    Thread.interrupted();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @SuppressWarnings({ "squid:S1181", "squid:S1193", "ConstantConditions", "UnreachableCode" })
    // catching Throwable, instanceofs, false-positive unreachable code
    public static void tryWithCleanupsAsHyracks(ThrowingAction action, ThrowingAction... cleanups)
            throws HyracksDataException {
        Throwable savedT = null;
        try {
            action.run();
        } catch (Throwable t) {
            savedT = t;
        } finally {
            for (ThrowingAction cleanup : cleanups) {
                try {
                    cleanup.run();
                } catch (Throwable t) {
                    savedT = ExceptionUtils.suppress(savedT, t);
                }
            }
        }
        if (Thread.interrupted()) {
            savedT = ExceptionUtils.suppress(savedT, new InterruptedException());
        }
        if (savedT == null) {
            return;
        }
        throw HyracksDataException.create(savedT);
    }

    @SuppressWarnings({ "squid:S1181", "squid:S1193", "ConstantConditions", "UnreachableCode" })
    // catching Throwable, instanceofs, false-positive unreachable code
    public static void tryWithCleanups(ThrowingAction action, ThrowingAction... cleanups) throws Exception {
        Throwable savedT = null;
        try {
            action.run();
        } catch (Throwable t) {
            savedT = t;
        } finally {
            for (ThrowingAction cleanup : cleanups) {
                try {
                    cleanup.run();
                } catch (Throwable t) {
                    savedT = ExceptionUtils.suppress(savedT, t);
                }
            }
        }
        if (Thread.interrupted()) {
            savedT = ExceptionUtils.suppress(savedT, new InterruptedException());
        }
        if (savedT == null) {
            return;
        }
        if (savedT instanceof Error) {
            throw (Error) savedT;
        } else if (savedT instanceof Exception) {
            throw (Exception) savedT;
        } else {
            throw HyracksDataException.create(savedT);
        }
    }

    /**
     * Runs the supplied action, and any specified cleanups. Any pending interruption will be cleared prior
     * to running the action and all cleanups. An error will be logged if the action and/or any of the cleanups
     * are themselves interrupted. Finally, if any action or cleanup was interrupted, or if the there was an
     * interrupt cleared as part of running any of these activities, either an InterruptedException will be returned
     * if the action & cleanups all ran without exception, or an InterruptedException will be suppressed into the
     * exception if the action or any of the cleanups threw an exception. In the case where InterruptedException is
     * suppressed, the current thread will be interrupted.
     *
     * @param action the action to run
     * @param cleanups the cleanups to run after the action
     * @return Exception if the action throws an exception or the action or any of the cleanups are interrupted, or if
     * the current thread was interrupted before running the action or any of the cleanups.
     */
    @SuppressWarnings({ "squid:S1181", "squid:S1193", "ConstantConditions", "UnreachableCode" })
    // catching Throwable, instanceofs, false-positive unreachable code
    public static Exception tryUninterruptibleWithCleanups(Exception root, ThrowingAction action,
            ThrowingAction... cleanups) {
        try {
            tryUninterruptibleWithCleanups(action, cleanups);
        } catch (Exception e) {
            root = ExceptionUtils.suppress(root, e);
        }
        return root;
    }

    /**
     * Runs the supplied action, and any specified cleanups. Any pending interruption will be cleared prior
     * to running the action and all cleanups. An error will be logged if the action and/or any of the cleanups
     * are themselves interrupted. Finally, if any action or cleanup was interrupted, or if the there was an
     * interrupt cleared as part of running any of these activities, either an InterruptedException will be thrown
     * if the action & cleanups all ran without exception, or an InterruptedException will be suppressed into the
     * exception if the action or any of the cleanups threw an exception. In the case where InterruptedException is
     * suppressed, the current thread will be interrupted.
     *
     * @param action the action to run
     * @param cleanups the cleanups to run after the action
     * @throws Exception if the action throws an exception or the action or any of the cleanups are interrupted.
     */
    @SuppressWarnings({ "squid:S1181", "squid:S1193", "ConstantConditions", "UnreachableCode" })
    // catching Throwable, instanceofs, false-positive unreachable code
    public static void tryUninterruptibleWithCleanups(ThrowingAction action, ThrowingAction... cleanups)
            throws Exception {
        Throwable savedT = null;
        try {
            runUninterruptible(action);
        } catch (Throwable t) {
            savedT = t;
        } finally {
            for (ThrowingAction cleanup : cleanups) {
                try {
                    runUninterruptible(cleanup);
                } catch (Throwable t) {
                    savedT = ExceptionUtils.suppress(savedT, t);
                }
            }
        }
        if (Thread.interrupted()) {
            savedT = ExceptionUtils.suppress(savedT, new InterruptedException());
        }
        if (savedT == null) {
            return;
        }
        if (savedT instanceof Error) {
            throw (Error) savedT;
        } else if (savedT instanceof Exception) {
            throw (Exception) savedT;
        } else {
            throw HyracksDataException.create(savedT);
        }
    }

    // catching Throwable, instanceofs, false-positive unreachable code
    public static void tryWithCleanups(ThrowingAction action, ThrowingConsumer<Throwable>... cleanups)
            throws Exception {
        Throwable savedT = null;
        boolean suppressedInterrupted = false;
        try {
            action.run();
        } catch (Throwable t) {
            savedT = t;
        } finally {
            for (ThrowingConsumer cleanup : cleanups) {
                try {
                    cleanup.process(savedT);
                } catch (Throwable t) {
                    if (savedT != null) {
                        savedT.addSuppressed(t);
                        suppressedInterrupted = suppressedInterrupted || t instanceof InterruptedException;
                    } else {
                        savedT = t;
                    }
                }
            }
        }
        if (savedT == null) {
            return;
        }
        if (suppressedInterrupted) {
            Thread.currentThread().interrupt();
        }
        if (savedT instanceof Error) {
            throw (Error) savedT;
        } else if (savedT instanceof Exception) {
            throw (Exception) savedT;
        } else {
            throw HyracksDataException.create(savedT);
        }
    }

    @SuppressWarnings({ "squid:S1181", "squid:S1193", "ConstantConditions" }) // catching Throwable, instanceofs
    public static void tryIoWithCleanups(IOThrowingAction action, IOThrowingAction... cleanups) throws IOException {
        Throwable savedT = null;
        boolean suppressedInterrupted = false;
        try {
            action.run();
        } catch (Throwable t) {
            savedT = t;
        } finally {
            for (IOThrowingAction cleanup : cleanups) {
                try {
                    cleanup.run();
                } catch (Throwable t) {
                    if (savedT != null) {
                        savedT.addSuppressed(t);
                        suppressedInterrupted = suppressedInterrupted || t instanceof InterruptedException;
                    } else {
                        savedT = t;
                    }
                }
            }
        }
        if (savedT == null) {
            return;
        }
        if (suppressedInterrupted) {
            Thread.currentThread().interrupt();
        }
        if (savedT instanceof Error) {
            throw (Error) savedT;
        } else if (savedT instanceof IOException) {
            throw (IOException) savedT;
        } else {
            throw HyracksDataException.create(savedT);
        }
    }

    @SuppressWarnings({ "squid:S1181", "squid:S1193", "ConstantConditions" }) // catching Throwable, instanceofs
    public static void tryHyracksWithCleanups(HyracksThrowingAction action, HyracksThrowingAction... cleanups)
            throws HyracksDataException {
        Throwable savedT = null;
        boolean suppressedInterrupted = false;
        try {
            action.run();
        } catch (Throwable t) {
            savedT = t;
        } finally {
            for (HyracksThrowingAction cleanup : cleanups) {
                try {
                    cleanup.run();
                } catch (Throwable t) {
                    if (savedT != null) {
                        savedT.addSuppressed(t);
                        suppressedInterrupted = suppressedInterrupted || t instanceof InterruptedException;
                    } else {
                        savedT = t;
                    }
                }
            }
        }
        if (savedT == null) {
            return;
        }
        if (suppressedInterrupted) {
            Thread.currentThread().interrupt();
        }
        if (savedT instanceof Error) {
            throw (Error) savedT;
        } else {
            throw HyracksDataException.create(savedT);
        }
    }

    public static void tryWithCleanupsUnchecked(Runnable action, Runnable... cleanups) {
        Throwable savedT = null;
        try {
            action.run();
        } catch (Throwable t) {
            savedT = t;
        } finally {
            for (Runnable cleanup : cleanups) {
                try {
                    cleanup.run();
                } catch (Throwable t) {
                    if (savedT != null) {
                        savedT.addSuppressed(t);
                    } else {
                        savedT = t;
                    }
                }
            }
        }
        if (savedT instanceof Error) {
            throw (Error) savedT;
        } else if (savedT != null) {
            throw new UncheckedExecutionException(savedT);
        }
    }

    /**
     * Runs the supplied action, after suspending any pending interruption. An error will be logged if
     * the action is itself interrupted.
     */
    public static Exception runUninterruptible(Exception root, ThrowingAction action) {
        try {
            runUninterruptible(action);
        } catch (Exception e) {
            root = ExceptionUtils.suppress(root, e);
        }
        return root;
    }

    /**
     * Runs the supplied action, after suspending any pending interruption. An error will be logged if
     * the action is itself interrupted.
     */
    public static void runUninterruptible(ThrowingAction action) throws Exception {
        boolean interrupted = Thread.interrupted();
        try {
            action.run();
            if (interrupted || Thread.interrupted()) {
                throw new InterruptedException();
            }
        } catch (InterruptedException e) {
            LOGGER.error("uninterruptible action {} was interrupted!", action, e);
            throw e;
        }
    }

    /**
     * Runs the supplied {@code action} until {@code stopCondition} is met or timeout.
     */
    public static void runWithTimeout(ThrowingAction action, BooleanSupplier stopCondition, long timeout, TimeUnit unit)
            throws Exception {
        final long waitTime = unit.toNanos(timeout);
        final long startTime = System.nanoTime();
        while (!stopCondition.getAsBoolean()) {
            action.run();
            if (System.nanoTime() - startTime >= waitTime) {
                throw new TimeoutException("Stop condition was not met after " + unit.toSeconds(timeout) + " seconds.");
            }
        }
    }

    public static <T> T retryUntilSuccessOrExhausted(Span span, ComputingAction<T> action, IRetryPolicy policy,
            IDelay delay) throws HyracksDataException {
        return retryUntilSuccessOrExhausted(span, action, policy, delay, defaultFailureCallback);
    }

    public static <T> T retryUntilSuccessOrExhausted(Span span, ComputingAction<T> action, IRetryPolicy policy,
            IDelay delay, IFailedAttemptCallback onFailure) throws HyracksDataException {
        Throwable failure;
        int attempt = 0;
        while (!Thread.currentThread().isInterrupted()) {
            attempt++;
            try {
                return action.compute();
            } catch (Throwable th) {
                failure = th;
                try {
                    long delayMs = delay.calculate(attempt);
                    if (!policy.retry(th) || span.elapsed() || span.remaining(TimeUnit.MILLISECONDS) < delayMs) {
                        onFailure.attemptFailed(action, attempt, true, span, failure);
                        if (th instanceof Error) {
                            throw (Error) th;
                        }
                        throw HyracksDataException.create(failure);
                    } else {
                        onFailure.attemptFailed(action, attempt, false, span, failure);
                    }
                    span.sleep(delayMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw HyracksDataException.create(e);
                }
            }
        }
        throw HyracksDataException.create(new InterruptedException());
    }

    public static Exception unwrapUnchecked(UncheckedExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Error err) {
            throw err;
        } else if (cause instanceof Exception ex) {
            return ex;
        } else {
            return HyracksDataException.create(cause);
        }
    }

    public static HyracksDataException unwrapUncheckedHyracks(UncheckedExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Error err) {
            throw err;
        } else {
            return HyracksDataException.create(cause);
        }
    }

    @FunctionalInterface
    public interface IFailedAttemptCallback {
        void attemptFailed(ComputingAction<?> action, int attempt, boolean isFinal, Span span, Throwable failure);
    }
}
