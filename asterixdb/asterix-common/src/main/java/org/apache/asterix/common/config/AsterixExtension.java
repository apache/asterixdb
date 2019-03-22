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
package org.apache.asterix.common.config;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.hyracks.algebricks.common.utils.Pair;

public class AsterixExtension {
    private final String className;
    private final List<Pair<String, String>> args;

    public AsterixExtension(String className, List<Pair<String, String>> args) {
        this.className = className;
        this.args = args;
    }

    public AsterixExtension(String className) {
        this.className = className;
        this.args = Collections.emptyList();
    }

    public List<Pair<String, String>> getArgs() {
        return args;
    }

    public String getClassName() {
        return className;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AsterixExtension) {
            AsterixExtension other = (AsterixExtension) o;
            return Objects.equals(className, other.className) && Objects.equals(args, other.args);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(className);
    }
}
