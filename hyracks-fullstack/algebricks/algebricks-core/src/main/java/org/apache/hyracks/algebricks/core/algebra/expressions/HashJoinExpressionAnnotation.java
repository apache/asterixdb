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

package org.apache.hyracks.algebricks.core.algebra.expressions;

import java.util.Objects;

import org.apache.hyracks.algebricks.common.utils.Pair;

public class HashJoinExpressionAnnotation implements IExpressionAnnotation {

    public enum BuildOrProbe {
        BUILD,
        PROBE
    }

    public enum BuildSide {
        LEFT,
        RIGHT
    }

    private BuildOrProbe buildOrProbe;
    private String name;
    private BuildSide side;

    public HashJoinExpressionAnnotation(Pair<BuildOrProbe, String> pair) {
        this.buildOrProbe = Objects.requireNonNull(pair.getFirst());
        this.name = validateName(pair.getSecond());
        this.side = null;
    }

    public HashJoinExpressionAnnotation(BuildSide side) {
        this.buildOrProbe = null;
        this.name = null;
        this.side = Objects.requireNonNull(side);
    }

    public BuildOrProbe getBuildOrProbe() {
        return buildOrProbe;
    }

    public String getName() {
        return name;
    }

    public BuildSide getBuildSide() {
        return side;
    }

    public void setBuildSide(BuildSide side) {
        this.buildOrProbe = null;
        this.name = null;
        this.side = Objects.requireNonNull(side);
    }

    private String validateName(String name) {
        String n = Objects.requireNonNull(name);
        if (n.isBlank()) {
            throw new IllegalArgumentException("HashJoinExpressionAnnotation:" + name + "cannot be blank");
        }
        return n;
    }
}