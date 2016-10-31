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
package org.apache.hyracks.api.exceptions;

import java.io.Serializable;
import java.util.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main execution time exception type for runtime errors in a hyracks environment
 */
public class HyracksDataException extends HyracksException {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(HyracksDataException.class.getName());

    public static final String NONE = "";
    public static final int UNKNOWN = 0;

    private final String component;
    private final int errorCode;
    private final Serializable[] params;
    private final String nodeId;
    private transient volatile String msgCache;

    public HyracksDataException(String component, int errorCode, String message, Throwable cause, String nodeId,
            Serializable... params) {
        super(message, cause);
        this.component = component;
        this.errorCode = errorCode;
        this.nodeId = nodeId;
        this.params = params;
    }

    public HyracksDataException(String message) {
        this(NONE, UNKNOWN, message, (Throwable) null, (String) null);
    }

    public HyracksDataException(Throwable cause) {
        this(NONE, UNKNOWN, cause.getMessage(), cause, (String) null);
    }

    public HyracksDataException(Throwable cause, String nodeId) {
        this(NONE, UNKNOWN, cause.getMessage(), cause, nodeId);
    }

    public HyracksDataException(String message, Throwable cause, String nodeId) {
        this(NONE, UNKNOWN, message, cause, nodeId);
    }

    public HyracksDataException(String message, Throwable cause) {
        this(NONE, UNKNOWN, message, cause, (String) null);
    }

    public HyracksDataException(String component, int errorCode, Serializable... params) {
        this(component, errorCode, null, null, null, params);
    }

    public HyracksDataException(Throwable cause, int errorCode, Serializable... params) {
        this(NONE, errorCode, cause.getMessage(), cause, null, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Serializable... params) {
        this(component, errorCode, message, null, null, params);
    }

    public HyracksDataException(String component, int errorCode, Throwable cause, Serializable... params) {
        this(component, errorCode, cause.getMessage(), cause, null, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Throwable cause,
            Serializable... params) {
        this(component, errorCode, message, cause, null, params);
    }

    public String getComponent() {
        return component;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public Object[] getParams() {
        return params;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public String getMessage() {
        if (msgCache == null) {
            msgCache = formatMessage(component, errorCode, super.getMessage(), params);
        }
        return msgCache;
    }

    /**
     * formats a error message
     * Example:
     * formatMessage(HYRACKS, ErrorCode.UNKNOWN, "%1$s -- %2$s", "one", "two") returns "HYR0000: one -- two"
     *
     * @param component
     *            the software component in which the error originated
     * @param errorCode
     *            the error code itself
     * @param message
     *            the user provided error message (a format string as specified in {@link java.util.Formatter})
     * @param params
     *            an array of objects taht will be provided to the {@link java.util.Formatter}
     * @return the formatted string
     */
    public static String formatMessage(String component, int errorCode, String message, Serializable... params) {
        try (Formatter fmt = new Formatter()) {
            if (!NONE.equals(component)) {
                fmt.format("%1$s%2$04d: ", component, errorCode);
            }
            fmt.format(message == null ? "null" : message, (Object[]) params);
            return fmt.out().toString();
        } catch (Exception e) {
            // Do not throw further exceptions during exception processing.
            LOGGER.log(Level.WARNING, e.getLocalizedMessage(), e);
            return e.getMessage();
        }
    }
}
