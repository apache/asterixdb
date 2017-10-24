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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.util.ErrorMessageUtil;

/**
 * The main execution time exception type for runtime errors in a hyracks environment
 */
public class HyracksDataException extends HyracksException {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(HyracksDataException.class.getName());

    public static HyracksDataException create(Throwable cause) {
        if (cause instanceof HyracksDataException || cause == null) {
            return (HyracksDataException) cause;
        } else if (cause instanceof Error) {
            // don't wrap errors, allow them to propagate
            throw (Error)cause;
        } else if (cause instanceof InterruptedException && !Thread.currentThread().isInterrupted()) {
            // TODO(mblow): why not force interrupt on current thread?
            LOGGER.log(Level.WARNING,
                    "Wrapping an InterruptedException in HyracksDataException and current thread is not interrupted",
                    cause);
        }
        return new HyracksDataException(cause);
    }

    public static HyracksDataException create(int code, Serializable... params) {
        return new HyracksDataException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), params);
    }

    public static HyracksDataException create(int code, Throwable cause, Serializable... params) {
        return new HyracksDataException(ErrorCode.HYRACKS, code, ErrorCode.getErrorMessage(code), cause, params);
    }

    public static HyracksDataException suppress(HyracksDataException root, Throwable th) {
        if (root == null) {
            return HyracksDataException.create(th);
        }
        if (th instanceof Error) {
            // don't suppress errors into a HyracksDataException, allow them to propagate
            th.addSuppressed(root);
            throw (Error) th;
        } else if (th instanceof InterruptedException && !Thread.currentThread().isInterrupted()) {
            // TODO(mblow): why not force interrupt on current thread?
            LOGGER.log(Level.WARNING, "Suppressing an InterruptedException in a HyracksDataException and current "
                    + "thread is not interrupted", th);
        }
        root.addSuppressed(th);
        return root;
    }

    public HyracksDataException(String component, int errorCode, String message, Throwable cause, String nodeId,
            Serializable... params) {
        super(component, errorCode, message, cause, nodeId, params);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksDataException(String message) {
        super(message);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksDataException(Throwable cause) {
        super(cause);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksDataException(Throwable cause, String nodeId) {
        super(cause, nodeId);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksDataException(String message, Throwable cause, String nodeId) {
        super(message, cause, nodeId);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public HyracksDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public HyracksDataException(String component, int errorCode, Serializable... params) {
        super(component, errorCode, null, null, null, params);
    }

    public HyracksDataException(Throwable cause, int errorCode, Serializable... params) {
        super(ErrorMessageUtil.NONE, errorCode, cause.getMessage(), cause, null, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Serializable... params) {
        super(component, errorCode, message, null, null, params);
    }

    public HyracksDataException(String component, int errorCode, Throwable cause, Serializable... params) {
        super(component, errorCode, cause, params);
    }

    public HyracksDataException(String component, int errorCode, String message, Throwable cause,
            Serializable... params) {
        super(component, errorCode, message, cause, null, params);
    }

    public static HyracksDataException create(HyracksDataException e, String nodeId) {
        return new HyracksDataException(e.getComponent(), e.getErrorCode(), e.getMessage(), e.getCause(), nodeId,
                e.getParams());
    }
}
