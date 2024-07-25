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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.util.ThrowingFunction;

import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * @author yingyib
 */
public class ExceptionUtils {

    private ExceptionUtils() {
    }

    /**
     * get a list of possible root causes from a list of all exceptions
     *
     * @param allExceptions
     * @return List of possible root causes
     */
    public static List<Exception> getActualExceptions(List<Exception> allExceptions) {
        List<Exception> exceptions = new ArrayList<>();
        for (Exception exception : allExceptions) {
            if (possibleRootCause(exception)) {
                exceptions.add(exception);
            }
        }
        return exceptions;
    }

    /**
     * Associate a node with a list of exceptions
     *
     * @param exceptions
     * @param nodeId
     */
    public static void setNodeIds(Collection<Exception> exceptions, String nodeId) {
        List<Exception> newExceptions = new ArrayList<>();
        for (Exception e : exceptions) {
            if (e instanceof HyracksDataException) {
                if (((HyracksDataException) e).getNodeId() == null) {
                    newExceptions.add(HyracksDataException.create((HyracksDataException) e, nodeId));
                } else {
                    newExceptions.add(e);
                }
            } else {
                newExceptions.add(HyracksDataException.create(ErrorCode.FAILURE_ON_NODE, e, nodeId));
            }
        }
        exceptions.clear();
        exceptions.addAll(newExceptions);
    }

    private static boolean possibleRootCause(Throwable exception) {
        Throwable cause = exception;
        while ((cause = cause.getCause()) != null) {
            if (cause instanceof java.lang.InterruptedException
                    || cause instanceof java.nio.channels.ClosedChannelException) {
                return false;
            }
        }
        return true;
    }

    /**
     * Suppress the second exception if not null into the first exception if not null.
     * If the suppressed exception is an instance of InterruptedException, the current thread is interrupted.
     *
     * @param first
     *            the root failure
     * @param second
     *            the subsequent failure
     * @return the root exception, or null if both parameters are null
     */
    public static <T extends Throwable> T suppress(T first, T second) {
        if (second instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        if (first == null) {
            return second;
        } else if (second == null) {
            return first;
        }
        if (first != second) {
            first.addSuppressed(second);
        }
        return first;
    }

    /**
     * Returns a throwable containing {@code thread} stacktrace
     *
     * @param thread
     * @return The throwable with {@code thread} stacktrace
     */
    public static Throwable fromThreadStack(Thread thread) {
        final Throwable stackThrowable = new Throwable(thread.getName() + " Stack trace");
        stackThrowable.setStackTrace(thread.getStackTrace());
        return stackThrowable;
    }

    public static Throwable getRootCause(Throwable e) {
        Throwable current = e;
        Throwable cause = e.getCause();
        while (cause != null && cause != current) {
            current = cause;
            cause = current.getCause();
        }
        return current;
    }

    public static boolean causedByInterrupt(Throwable th) {
        return getRootCause(th) instanceof InterruptedException;
    }

    /**
     * Convenience utility method to provide a form of {@link Map#computeIfAbsent(Object, Function)} which allows
     * throwable mapping functions.  Any exceptions thrown by the mapping function is propagated as an instance of
     * {@link HyracksDataException}
     */
    public static <K, V> V computeIfAbsent(Map<K, V> map, K key, ThrowingFunction<K, V> function)
            throws HyracksDataException {
        try {
            return map.computeIfAbsent(key, k -> {
                try {
                    return function.process(k);
                } catch (Exception e) {
                    throw new UncheckedExecutionException(e);
                }
            });
        } catch (UncheckedExecutionException e) {
            throw HyracksDataException.create(e.getCause());
        }
    }

    // Gets the error message for the root cause of a given Throwable instance.
    public static String getErrorMessage(Throwable th) {
        Throwable cause = getRootCause(th);
        return cause.getMessage();
    }

    /**
     * Determines whether supplied exception contains a matching cause in its hierarchy, or is itself a match
     */
    public static boolean matchingCause(Throwable e, Predicate<Throwable> test) {
        Throwable current = e;
        Throwable cause = e.getCause();
        while (cause != null && cause != current) {
            if (test.test(cause)) {
                return true;
            }
            Throwable nextCause = current.getCause();
            current = cause;
            cause = nextCause;
        }
        return test.test(e);
    }

    /**
     * Unwraps enclosed exceptions until a non-product exception is found, otherwise returns the root production
     * exception
     */
    public static Throwable unwrap(Throwable e) {
        Throwable current = e;
        Throwable cause = e.getCause();
        while (cause != current && cause instanceof IFormattedException) {
            current = cause;
            cause = current.getCause();
        }
        return current;
    }

    /**
     * Returns the message of the throwable if of type IFormattedException, otherwise, .toString() is returned
     *
     * @param e throwable
     * @return error message
     */
    public static String getMessageOrToString(Throwable e) {
        return e instanceof IFormattedException ? e.getMessage() : e.toString();
    }

    /**
     * Checks if the error code of the throwable is of the provided type
     *
     * @param throwable throwable with error code
     * @param code error code to match against
     *
     * @return true if error code matches, false otherwise
     */
    public static boolean isErrorCode(HyracksDataException throwable, ErrorCode code) {
        return throwable.getError().isPresent() && throwable.getError().get() == code;
    }
}
