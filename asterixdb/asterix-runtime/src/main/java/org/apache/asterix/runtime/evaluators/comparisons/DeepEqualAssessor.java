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
package org.apache.asterix.runtime.evaluators.comparisons;

import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.Domain;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.evaluators.visitors.DeepEqualityVisitor;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Use {@link DeepEqualityVisitor} to assess the deep equality between two
 * pointable values, including oredered and unordered lists, record values, etc.
 * Example: Let IVisitablePointable leftPointable, IVisitablePointable rightPointable be two
 * value references. To assess their equality, simply use
 * DeepEqualAssessor dea = new DeepEqualAssessor();
 * boolean isEqual = dea.isEqual(leftPointable, rightPointable);
 */

public class DeepEqualAssessor {
    private static final String DEEP_EQUAL = "deep-equal";
    private static final double EPSILON = 1E-10;
    private final DeepEqualityVisitor equalityVisitor = new DeepEqualityVisitor();

    public boolean isEqual(IVisitablePointable leftPointable, IVisitablePointable rightPointable)
            throws HyracksDataException {
        if (leftPointable == null || rightPointable == null) {
            return false;
        }

        if (leftPointable.equals(rightPointable)) {
            return true;
        }

        ATypeTag leftTypeTag = PointableHelper.getTypeTag(leftPointable);
        ATypeTag rightTypeTag = PointableHelper.getTypeTag(rightPointable);

        if (leftTypeTag != rightTypeTag) {
            // If types are numeric compare their real values instead
            if (ATypeHierarchy.isSameTypeDomain(leftTypeTag, rightTypeTag, false)
                    && ATypeHierarchy.getTypeDomain(leftTypeTag) == Domain.NUMERIC) {
                double leftVal = ATypeHierarchy.getDoubleValue(DEEP_EQUAL, 0, leftPointable.getByteArray(),
                        leftPointable.getStartOffset());
                double rightVal = ATypeHierarchy.getDoubleValue(DEEP_EQUAL, 1, rightPointable.getByteArray(),
                        rightPointable.getStartOffset());
                return Math.abs(leftVal - rightVal) < EPSILON;
            } else {
                return false;
            }
        }

        Pair<IVisitablePointable, Boolean> arg = new Pair<IVisitablePointable, Boolean>(rightPointable, Boolean.FALSE);
        // Assess the nested equality
        leftPointable.accept(equalityVisitor, arg);

        return arg.second;
    }
}
