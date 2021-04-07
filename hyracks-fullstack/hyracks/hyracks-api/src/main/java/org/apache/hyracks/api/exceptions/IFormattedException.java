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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

public interface IFormattedException {

    /**
     * Gets the component of this {@link IFormattedException}
     *
     * @return the exception component
     */
    String getComponent();

    /**
     * Gets the error code of this {@link IFormattedException}
     *
     * @return the error code
     */
    int getErrorCode();

    /**
     * Gets the message of this exception
     *
     * @return the exception message
     */
    String getMessage();

    /**
     * See {@link Throwable#getSuppressed()}
     */
    Throwable[] getSuppressed();

    /**
     * If available, returns the {@link IError} associated with this exception
     * @return the error instance, otherwise {@link Optional#empty()}
     * @since 0.3.5.1
     */
    Optional<IError> getError();

    /**
     * @return the source location
     */
    SourceLocation getSourceLocation();

    /**
     * @return the parameters to use when formatting
     */
    Serializable[] getParams();

    /**
     * Indicates whether this exception matches the supplied error code
     */
    default boolean matches(IError candidate) {
        Objects.requireNonNull(candidate, "candidate");
        return getComponent().equals(candidate.component()) && getErrorCode() == candidate.intValue();
    }

    /**
     * Indicates whether this exception matches any of the supplied error codes
     */
    default boolean matchesAny(IError candidate, IError... otherCandidates) {
        return matches(candidate) || Stream.of(otherCandidates).anyMatch(this::matches);
    }

    /**
     * Tests for matching component & errorCode against supplied throwable
     *
     * @param candidate the error type to match
     * @return <code>true</code> if the supplied {@link Throwable} matches the supplied candidate
     */
    static boolean matches(Throwable th, IError candidate) {
        return th instanceof IFormattedException && ((IFormattedException) th).matches(candidate);
    }

    /**
     * Tests for matching component & errorCode against supplied throwable
     *
     * @return <code>true</code> if the supplied {@link Throwable} matches any of the supplied candidates
     */
    static boolean matchesAny(Throwable th, IError candidate, IError... otherCandidates) {
        return th instanceof IFormattedException && ((IFormattedException) th).matchesAny(candidate, otherCandidates);
    }

    /**
     * If the supplied {@link Throwable} is an instance of {@link IFormattedException}, return the {@link IError}
     * associated with this exception if available
     *
     * @return the error instance, otherwise {@link Optional#empty()}
     * @since 0.3.5.1
     */
    static Optional<IError> getError(Throwable throwable) {
        return throwable instanceof IFormattedException ? ((IFormattedException) throwable).getError()
                : Optional.empty();
    }
}
