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
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;

public class IntervalJoinExpressionAnnotation implements IExpressionAnnotation {

    public static final String IOP_HINT_STRING = "interval-iop-join";
    public static final String MERGE_HINT_STRING = "interval-merge-join";
    public static final String SPATIAL_HINT_STRING = "interval-spatial-join";
    public static final IntervalJoinExpressionAnnotation INSTANCE = new IntervalJoinExpressionAnnotation();

    private Object object;
    private IRangeMap map;
    private String joinType;

    @Override
    public Object getObject() {
        return object;
    }

    @Override
    public void setObject(Object object) {
        this.object = object;
    }

    @Override
    public IExpressionAnnotation copy() {
        IntervalJoinExpressionAnnotation clone = new IntervalJoinExpressionAnnotation();
        clone.setObject(object);
        return clone;
    }

    public void setRangeMap(IRangeMap map) {
        this.map = map;
    }

    public IRangeMap getRangeMap() {
        return map;
    }

    public void setJoinType(String hint) {
        if (hint.startsWith(IOP_HINT_STRING)) {
            joinType = IOP_HINT_STRING;
        } else if (hint.startsWith(MERGE_HINT_STRING)) {
            joinType = MERGE_HINT_STRING;
        } else if (hint.startsWith(SPATIAL_HINT_STRING)) {
            joinType = SPATIAL_HINT_STRING;
        }
    }

    public String getRangeType() {
        return joinType;
    }

    public boolean isIopJoin() {
        if (joinType.equals(IOP_HINT_STRING)) {
            return true;
        }
        return false;
    }

    public boolean isMergeJoin() {
        if (joinType.equals(MERGE_HINT_STRING)) {
            return true;
        }
        return false;
    }

    public boolean isSpatialJoin() {
        if (joinType.equals(SPATIAL_HINT_STRING)) {
            return true;
        }
        return false;
    }

    public static boolean isIntervalJoinHint(String hint) {
        if (hint.startsWith(IOP_HINT_STRING) || hint.startsWith(MERGE_HINT_STRING)
                || hint.startsWith(SPATIAL_HINT_STRING)) {
            return true;
        } else {
            return false;
        }
    }

    public static int getHintLength(String hint) {
        if (hint.startsWith(IOP_HINT_STRING)) {
            return IOP_HINT_STRING.length();
        } else if (hint.startsWith(MERGE_HINT_STRING)) {
            return MERGE_HINT_STRING.length();
        } else if (hint.startsWith(SPATIAL_HINT_STRING)) {
            return SPATIAL_HINT_STRING.length();
        }
        return 0;
    }

}
