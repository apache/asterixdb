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
package org.apache.asterix.external.library;

import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;

final class PythonLibraryEvaluatorId {

    private final DataverseName libraryDataverseName;

    private final String libraryName;

    private final Thread thread;

    PythonLibraryEvaluatorId(DataverseName libraryDataverseName, String libraryName, Thread thread) {
        this.libraryDataverseName = Objects.requireNonNull(libraryDataverseName);
        this.libraryName = Objects.requireNonNull(libraryName);
        this.thread = Objects.requireNonNull(thread);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PythonLibraryEvaluatorId that = (PythonLibraryEvaluatorId) o;
        return libraryDataverseName.equals(that.libraryDataverseName) && libraryName.equals(that.libraryName)
                && thread.equals(that.thread);
    }

    @Override
    public int hashCode() {
        return Objects.hash(libraryDataverseName, libraryName);
    }

    public DataverseName getLibraryDataverseName() {
        return libraryDataverseName;
    }

    public String getLibraryName() {
        return libraryName;
    }

}
