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

import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;

public class SortMergeJoinExpressionAnnotation implements IExpressionAnnotation {
    public static final String SORT_MERGE_JOIN_EXPRESSION_ANNOTATION = "smjoin";

    public static final ComparisonKind[][] thetaMatrix = { { null }, { ComparisonKind.GE, ComparisonKind.GT },
            { ComparisonKind.LE, ComparisonKind.LT }, { ComparisonKind.GE, ComparisonKind.GT },
            { ComparisonKind.LE, ComparisonKind.LT }, { null } };

    public static final ComparisonKind[] switchMapping = { ComparisonKind.NEQ, ComparisonKind.GT, ComparisonKind.LT,
            ComparisonKind.GE, ComparisonKind.LE, ComparisonKind.EQ };

    public enum SortMergeJoinType {
        BAND,
        THETA,
        METRIC,
        SKYLINE,
        NESTLOOP
    }

    SortMergeJoinType type;

    @Override
    public Object getObject() {
        // TODO Auto-generated method stub
        return type;
    }

    @Override
    public void setObject(Object object) {
        // TODO Auto-generated method stub
        this.type = (SortMergeJoinType) object;
    }

    @Override
    public IExpressionAnnotation copy() {
        // TODO Auto-generated method stub
        SortMergeJoinExpressionAnnotation smjoin = new SortMergeJoinExpressionAnnotation();
        smjoin.type = type;
        return smjoin;
    }

}
