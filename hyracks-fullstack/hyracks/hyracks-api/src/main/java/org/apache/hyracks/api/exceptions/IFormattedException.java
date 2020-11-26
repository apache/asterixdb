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

import java.util.Objects;

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
     * Tests for matching component & errorCode against this exception
     *
     * @param component the component to match
     * @param errorCode the errorCode to match
     * @return <code>true</code> if this {@link IFormattedException} instance matches the supplied parameters
     */
    default boolean matches(String component, int errorCode) {
        Objects.requireNonNull(component, "component");
        return component.equals(getComponent()) && errorCode == getErrorCode();
    }

    /**
     * Tests for matching component & errorCode against supplied throwable
     *
     * @param component the component to match
     * @param errorCode the errorCode to match
     * @return <code>true</code> if the supplied {@link Throwable}  matches the supplied parameters
     */
    static boolean matches(Throwable th, String component, int errorCode) {
        return th instanceof IFormattedException && ((IFormattedException) th).matches(component, errorCode);
    }
}