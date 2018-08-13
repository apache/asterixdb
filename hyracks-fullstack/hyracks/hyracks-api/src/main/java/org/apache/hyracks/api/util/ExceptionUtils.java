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

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;

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
                newExceptions.add(new HyracksDataException(ErrorCode.HYRACKS, ErrorCode.FAILURE_ON_NODE, e, nodeId));
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
    public static Throwable suppress(Throwable first, Throwable second) {
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
}
