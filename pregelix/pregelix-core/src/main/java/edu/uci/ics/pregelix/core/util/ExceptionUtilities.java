/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.pregelix.core.util;

import java.io.IOException;
import java.util.Set;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * The util to analysis exceptions
 * 
 * @author yingyib
 */
public class ExceptionUtilities {

    /**
     * Check whether a exception is recoverable or not
     * 
     * @param exception
     * @return true or false
     */
    public static boolean recoverable(Exception exception, Set<String> blackListNodes) {
        String message = exception.getMessage();

        /***
         * check interrupted exception
         */
        if (exception instanceof InterruptedException || (message.contains("Node") && message.contains("not live"))
                || message.contains("Failure occurred on input")) {
            return true;
        }
        Throwable cause = exception;
        while ((cause = cause.getCause()) != null) {
            if (cause instanceof InterruptedException) {
                return true;
            }
        }

        /***
         * check io exception
         */
        cause = exception;
        String blackListNode = null;
        if (cause instanceof HyracksDataException) {
            blackListNode = ((HyracksDataException) cause).getNodeId();
        }
        while ((cause = cause.getCause()) != null) {
            if (cause instanceof IOException) {
                if (containsIOManager(cause)) {
                    if (blackListNode != null) {
                        blackListNodes.add(blackListNode);
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if the exception traces contains the IOManager, which means there are disk failures
     * 
     * @param cause
     * @return true if IOManager is in the trace; false otherwise.
     */
    private static boolean containsIOManager(Throwable cause) {
        StackTraceElement[] traces = cause.getStackTrace();
        for (StackTraceElement e : traces) {
            if (e.getClassName().endsWith("IOManager")) {
                return true;
            }
        }
        return false;
    }
}
