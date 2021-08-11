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
package org.apache.hyracks.util;

import java.util.NoSuchElementException;

/**
 * Provides functionality similar to {@link java.util.Optional} for the primitive boolean type
 */
public class OptionalBoolean {
    private static final OptionalBoolean EMPTY = new OptionalBoolean(false) {
        @Override
        public boolean isPresent() {
            return false;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public boolean equals(Object o) {
            return o == this;
        }

        @Override
        public int hashCode() {
            return -1;
        }

        @Override
        public boolean get() {
            throw new NoSuchElementException();
        }
    };

    private static final OptionalBoolean TRUE = new OptionalBoolean(true);
    private static final OptionalBoolean FALSE = new OptionalBoolean(false);

    private final boolean value;

    public static OptionalBoolean of(boolean value) {
        return value ? TRUE : FALSE;
    }

    public static OptionalBoolean ofNullable(Boolean value) {
        return value == null ? EMPTY : value ? TRUE : FALSE;
    }

    public static OptionalBoolean empty() {
        return EMPTY;
    }

    public static OptionalBoolean TRUE() {
        return TRUE;
    }

    public static OptionalBoolean FALSE() {
        return FALSE;
    }

    private OptionalBoolean(boolean value) {
        this.value = value;
    }

    public boolean isPresent() {
        return true;
    }

    public boolean isEmpty() {
        return false;
    }

    public boolean get() {
        return value;
    }

    public boolean getOrElse(boolean alternate) {
        return isPresent() ? get() : alternate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (this == EMPTY || o == EMPTY)
            return false;
        if (o == null || getClass() != o.getClass())
            return false;
        OptionalBoolean that = (OptionalBoolean) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return value ? 1 : 0;
    }
}
