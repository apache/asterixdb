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
package org.apache.asterix.runtime.evaluators.visitors;

import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.Domain;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class DeepEqualityVisitor implements IVisitablePointableVisitor<Void, Pair<IVisitablePointable, Boolean>> {
    private final Map<IVisitablePointable, ListDeepEqualityChecker> lpointableToEquality = new HashMap<>();
    private final Map<IVisitablePointable, RecordDeepEqualityChecker> rpointableToEquality = new HashMap<>();

    @Override
    public Void visit(AListVisitablePointable pointable, Pair<IVisitablePointable, Boolean> arg)
            throws HyracksDataException {
        ListDeepEqualityChecker listDeepEqualityChecker = lpointableToEquality.get(pointable);
        if (listDeepEqualityChecker == null) {
            listDeepEqualityChecker = new ListDeepEqualityChecker();
            lpointableToEquality.put(pointable, listDeepEqualityChecker);
        }

        arg.second = listDeepEqualityChecker.accessList(pointable, arg.first, this);

        return null;
    }

    @Override
    public Void visit(ARecordVisitablePointable pointable, Pair<IVisitablePointable, Boolean> arg)
            throws HyracksDataException {
        RecordDeepEqualityChecker recDeepEqualityChecker = rpointableToEquality.get(pointable);
        if (recDeepEqualityChecker == null) {
            recDeepEqualityChecker = new RecordDeepEqualityChecker();
            rpointableToEquality.put(pointable, recDeepEqualityChecker);
        }

        arg.second = recDeepEqualityChecker.accessRecord(pointable, arg.first, this);

        return null;
    }

    @Override
    public Void visit(AFlatValuePointable pointable, Pair<IVisitablePointable, Boolean> arg)
            throws HyracksDataException {

        if (pointable.equals(arg.first)) {
            arg.second = true;
            return null;
        }
        ATypeTag tt1 = PointableHelper.getTypeTag(pointable);
        ATypeTag tt2 = PointableHelper.getTypeTag(arg.first);

        if (tt1 != tt2) {
            if (!ATypeHierarchy.isSameTypeDomain(tt1, tt2, false)) {
                arg.second = false;
            } else {
                // If same domain, check if numberic
                Domain domain = ATypeHierarchy.getTypeDomain(tt1);
                byte b1[] = pointable.getByteArray();
                byte b2[] = arg.first.getByteArray();
                if (domain == Domain.NUMERIC) {
                    int s1 = pointable.getStartOffset();
                    int s2 = arg.first.getStartOffset();
                    arg.second = Math.abs(ATypeHierarchy.getDoubleValue("deep-equal", 0, b1, s1)
                            - ATypeHierarchy.getDoubleValue("deep-equal", 1, b2, s2)) < 1E-10;
                } else {
                    arg.second = false;
                }
            }
        } else {
            arg.second = PointableHelper.byteArrayEqual(pointable, arg.first, 1);
        }
        return null;
    }

}
