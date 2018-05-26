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
package org.apache.asterix.translator.util;

import java.util.List;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IVariableContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class PlanTranslationUtil {
    private static final LogicalVariable DUMMY_VAR = new LogicalVariable(-1);

    public static void prepareMetaKeyAccessExpression(List<String> field, LogicalVariable resVar,
            List<Mutable<ILogicalExpression>> assignExpressions, List<LogicalVariable> vars,
            List<Mutable<ILogicalExpression>> varRefs, IVariableContext context, SourceLocation sourceLoc) {
        IAObject value = (field.size() > 1) ? new AOrderedList(field) : new AString(field.get(0));
        ScalarFunctionCallExpression metaKeyFunction =
                new ScalarFunctionCallExpression(FunctionUtil.getFunctionInfo(BuiltinFunctions.META_KEY));
        metaKeyFunction.setSourceLocation(sourceLoc);
        VariableReferenceExpression resVarRef = new VariableReferenceExpression(resVar);
        resVarRef.setSourceLocation(sourceLoc);
        metaKeyFunction.getArguments().add(new MutableObject<ILogicalExpression>(resVarRef));
        metaKeyFunction.getArguments()
                .add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(value))));
        assignExpressions.add(new MutableObject<ILogicalExpression>(metaKeyFunction));
        LogicalVariable v = context.newVar();
        vars.add(v);
        if (varRefs != null) {
            VariableReferenceExpression vRef = new VariableReferenceExpression(v);
            vRef.setSourceLocation(sourceLoc);
            varRefs.add(new MutableObject<ILogicalExpression>(vRef));
        }
    }

    public static void prepareVarAndExpression(List<String> field, LogicalVariable resVar, List<LogicalVariable> vars,
            List<Mutable<ILogicalExpression>> assignExpressions, List<Mutable<ILogicalExpression>> varRefs,
            IVariableContext context, SourceLocation sourceLoc) {
        VariableReferenceExpression dummyVarRef = new VariableReferenceExpression(DUMMY_VAR);
        dummyVarRef.setSourceLocation(sourceLoc);
        ScalarFunctionCallExpression f = createFieldAccessExpression(dummyVarRef, field, sourceLoc);
        f.substituteVar(DUMMY_VAR, resVar);
        assignExpressions.add(new MutableObject<ILogicalExpression>(f));
        LogicalVariable v = context.newVar();
        vars.add(v);
        if (varRefs != null) {
            VariableReferenceExpression vRef = new VariableReferenceExpression(v);
            vRef.setSourceLocation(sourceLoc);
            varRefs.add(new MutableObject<ILogicalExpression>(vRef));
        }
    }

    private static ScalarFunctionCallExpression createFieldAccessExpression(ILogicalExpression target,
            List<String> field, SourceLocation sourceLoc) {
        FunctionIdentifier functionIdentifier;
        IAObject value;
        if (field.size() > 1) {
            functionIdentifier = BuiltinFunctions.FIELD_ACCESS_NESTED;
            value = new AOrderedList(field);
        } else {
            functionIdentifier = BuiltinFunctions.FIELD_ACCESS_BY_NAME;
            value = new AString(field.get(0));
        }
        IFunctionInfo finfoAccess = FunctionUtil.getFunctionInfo(functionIdentifier);
        ScalarFunctionCallExpression faExpr = new ScalarFunctionCallExpression(finfoAccess, new MutableObject<>(target),
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(value))));
        faExpr.setSourceLocation(sourceLoc);
        return faExpr;
    }
}
