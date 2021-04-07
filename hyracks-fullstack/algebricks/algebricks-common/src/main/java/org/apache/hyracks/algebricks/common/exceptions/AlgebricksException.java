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
package org.apache.hyracks.algebricks.common.exceptions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Optional;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.IError;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.util.ErrorMessageUtil;

public class AlgebricksException extends Exception implements IFormattedException {
    private static final long serialVersionUID = 2L;

    public static final int UNKNOWN = 0;
    private final String component;
    private final int errorCode;
    private final Serializable[] params;
    private final String nodeId;
    private final SourceLocation sourceLoc;
    protected transient IError error;

    @SuppressWarnings("squid:S1165") // exception class not final
    private transient volatile String msgCache;

    public static AlgebricksException create(ErrorCode error, SourceLocation sourceLoc, Serializable... params) {
        return new AlgebricksException(error, sourceLoc, params);
    }

    public static AlgebricksException create(ErrorCode error, Serializable... params) {
        return create(error, null, params);
    }

    protected AlgebricksException(IError error, String component, int errorCode, String message, Throwable cause,
            SourceLocation sourceLoc, String nodeId, Serializable... params) {
        super(message, cause);
        this.error = error;
        this.component = component;
        this.errorCode = errorCode;
        this.sourceLoc = sourceLoc;
        this.nodeId = nodeId;
        this.params = params;
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public AlgebricksException(String message) {
        this((IError) null, ErrorMessageUtil.NONE, UNKNOWN, message, null, null, null);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public AlgebricksException(Throwable cause) {
        this(String.valueOf(cause), cause);
    }

    /**
     * @deprecated Error code is needed.
     */
    @Deprecated
    public AlgebricksException(String message, Throwable cause) {
        this((IError) null, ErrorMessageUtil.NONE, UNKNOWN, message, cause, null, null);
    }

    public AlgebricksException(Throwable cause, ErrorCode error, Serializable... params) {
        this(error, error.component(), error.intValue(), error.errorMessage(), cause, null, null, params);
    }

    public AlgebricksException(ErrorCode error, SourceLocation sourceLoc, Serializable... params) {
        this(error, error.component(), error.intValue(), error.errorMessage(), null, sourceLoc, null, params);
    }

    public AlgebricksException(ErrorCode error, Serializable... params) {
        this(error, null, params);
    }

    protected AlgebricksException(IError error, Throwable cause, SourceLocation sourceLoc, Serializable... params) {
        this(error, error.component(), error.intValue(), error.errorMessage(), cause, sourceLoc, null, params);
    }

    @Override
    public String getComponent() {
        return component;
    }

    @Override
    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public Serializable[] getParams() {
        return params;
    }

    public String getNodeId() {
        return nodeId;
    }

    @Override
    public SourceLocation getSourceLocation() {
        return sourceLoc;
    }

    @Override
    public Optional<IError> getError() {
        return Optional.ofNullable(error);
    }

    @Override
    public String getMessage() {
        if (msgCache == null) {
            msgCache = ErrorMessageUtil.formatMessage(component, errorCode, super.getMessage(), sourceLoc, params);
        }
        return msgCache;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        ErrorMessageUtil.writeObjectWithError(error, out);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        error = ErrorMessageUtil.readObjectWithError(in).orElse(null);
    }
}
