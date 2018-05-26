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

package org.apache.asterix.common.exceptions;

import java.io.Serializable;

import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetadataException extends CompilationException {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();

    @Deprecated
    /**
     * @Deprecated Instead, use a constructor with error code
     * @param message
     */
    public MetadataException(String message) {
        super(message);
    }

    @Deprecated
    /**
     * @Deprecated When creating a constructor with cause,
     *             create AlgebricksException using AlgebricksException.create(Throwable th);
     * @param cause
     */
    public MetadataException(Throwable cause) {
        super(cause);
    }

    @Deprecated
    /**
     * @Deprecated When creating a constructor with cause,
     *             create AlgebricksException using AlgebricksException.create(Throwable th);
     * @param cause
     */
    public MetadataException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetadataException(int errorCode, Serializable... params) {
        super(errorCode, params);
    }

    public MetadataException(int errorCode, SourceLocation sourceLoc, Serializable... params) {
        super(errorCode, sourceLoc, params);
    }

    public MetadataException(int errorCode, Throwable cause, Serializable... params) {
        super(errorCode, cause, params);
    }

    public MetadataException(int errorCode, Throwable cause, SourceLocation sourceLoc, Serializable... params) {
        super(errorCode, cause, sourceLoc, params);
    }

    public static MetadataException create(Throwable cause) {
        if (cause instanceof MetadataException || cause == null) {
            return (MetadataException) cause;
        }
        if (cause instanceof InterruptedException && !Thread.currentThread().isInterrupted()) {
            LOGGER.log(Level.WARN, "Wrapping an InterruptedException in " + MetadataException.class.getSimpleName()
                    + " and current thread is not interrupted", cause);
        }
        return new MetadataException(cause);
    }
}
