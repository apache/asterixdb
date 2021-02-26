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
package org.apache.asterix.common.annotations;

import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public final class SpatialJoinAnnotation implements IExpressionAnnotation {

    public static final String HINT_STRING = "spatial-partitioning";

    private final double minX;
    private final double minY;
    private final double maxX;
    private final double maxY;
    private final int numRows;
    private final int numColumns;

    public SpatialJoinAnnotation(double minX, double minY, double maxX, double maxY, int numRows, int numColumns) {
        this.minX = minX;
        this.minY = minY;
        this.maxX = maxX;
        this.maxY = maxY;
        this.numRows = numRows;
        this.numColumns = numColumns;
    }

    @Override
    public String toString() {
        return String.format("%s:%f,%f,%f,%f,%d,%d", HINT_STRING, getMinX(), getMinY(), getMaxX(), getMaxY(),
                getNumRows(), getNumColumns());
    }

    public double getMinX() {
        return minX;
    }

    public double getMinY() {
        return minY;
    }

    public double getMaxX() {
        return maxX;
    }

    public double getMaxY() {
        return maxY;
    }

    public int getNumRows() {
        return numRows;
    }

    public int getNumColumns() {
        return numColumns;
    }
}
