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
package org.apache.hyracks.algebricks.core.algebra.base;

/**
 * Represents a logical variable in an asterix logical plan.
 *
 * @author Vinayak R. Borkar [vborkar@ics.uci.edu]
 */
public final class LogicalVariable {
    private final int id;
    private final String displayName;

    public LogicalVariable(int id) {
        this.id = id;
        this.displayName = "$$" + id;
    }

    public LogicalVariable(int id, String displayName) {
        this.id = id;
        this.displayName = "$" + displayName;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return displayName;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LogicalVariable) {
            return id == ((LogicalVariable) obj).getId();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
