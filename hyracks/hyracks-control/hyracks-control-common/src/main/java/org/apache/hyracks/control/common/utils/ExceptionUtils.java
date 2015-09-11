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
package org.apache.hyracks.control.common.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * @author yingyib
 */
public class ExceptionUtils {

    public static List<Exception> getActualExceptions(List<Exception> allExceptions) {
        List<Exception> exceptions = new ArrayList<Exception>();
        for (Exception exception : allExceptions) {
            if (possibleRootCause(exception)) {
                exceptions.add(exception);
            }
        }
        return exceptions;
    }

    public static void setNodeIds(Collection<Exception> exceptions, String nodeId) {
        List<Exception> newExceptions = new ArrayList<Exception>();
        for (Exception e : exceptions) {
            HyracksDataException newException = new HyracksDataException(e);
            newException.setNodeId(nodeId);
            newExceptions.add(newException);
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

}
