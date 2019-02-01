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

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class CompatibilityLevel implements Serializable, Comparable {
    public static final CompatibilityLevel V_0_3_4 = fromSegments(0, 3, 4);
    public static final CompatibilityLevel V_0_3_5 = fromSegments(0, 3, 5);

    public static final CompatibilityLevel DEFAULT = V_0_3_5;

    private final int level;

    private CompatibilityLevel(int level) {
        this.level = level;
    }

    public static CompatibilityLevel fromInt(int version) {
        return new CompatibilityLevel(version);
    }

    public static CompatibilityLevel fromSegments(int... versionSegments) {
        if (versionSegments.length > 4) {
            throw new IllegalArgumentException(
                    "a maximum of four version segments is supported; (was: " + Arrays.toString(versionSegments) + ")");
        }
        if (IntStream.of(versionSegments).anyMatch(i -> i > 0xff)) {
            throw new IllegalArgumentException(
                    "a version segment cannot exceed 255 (was: " + Arrays.toString(versionSegments) + ")");
        }
        int version = (versionSegments[0] & 0xff) << 24;
        if (versionSegments.length > 1) {
            version |= (versionSegments[1] & 0xff) << 16;
        }
        if (versionSegments.length > 2) {
            version |= (versionSegments[2] & 0xff) << 8;
        }
        if (versionSegments.length > 3) {
            version |= versionSegments[3] & 0xff;
        }
        return fromInt(version);
    }

    @Override
    public int compareTo(Object o) {
        return Integer.compareUnsigned(level, ((CompatibilityLevel) o).level);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompatibilityLevel that = (CompatibilityLevel) o;
        return level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    private Object writeReplace() throws ObjectStreamException {
        return this;
    }

    public int intValue() {
        return level;
    }
}
