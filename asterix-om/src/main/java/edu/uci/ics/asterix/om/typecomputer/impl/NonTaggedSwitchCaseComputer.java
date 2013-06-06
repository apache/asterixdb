/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonTaggedSwitchCaseComputer implements IResultTypeComputer {

    private static final String errMsg1 = "switch case should have at least 3 parameters ";
    private static final String errMsg2 = "every case expression should have the same return type";
    private static final String errMsg3 = "swith conditon type should be compatible with each case type";

    public static IResultTypeComputer INSTANCE = new NonTaggedSwitchCaseComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().size() < 3)
            throw new AlgebricksException(errMsg1);

        IAType t0;
        IAType t1;
        IAType ti;

        ATypeTag tag0;
        ATypeTag tag1;
        ATypeTag tagi;
        try {
            t0 = (IAType) env.getType(fce.getArguments().get(0).getValue());
            t1 = (IAType) env.getType(fce.getArguments().get(2).getValue());
            tag0 = t0.getTypeTag();
            tag1 = t1.getTypeTag();
            if (t0.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t0))
                tag0 = ((AUnionType) t0).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                        .getTypeTag();
            if (t1.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t1))
                tag1 = ((AUnionType) t1).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                        .getTypeTag();
            for (int i = 2; i < fce.getArguments().size(); i += 2) {
                ti = (IAType) env.getType(fce.getArguments().get(i).getValue());
                tagi = ti.getTypeTag();
                if (ti.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) ti))
                    tagi = ((AUnionType) ti).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                            .getTypeTag();
                if (tag1 != tagi)
                    if (!t1.toString().equals(ti.toString()))
                        throw new AlgebricksException(errMsg2);
            }
            for (int i = 1; i < fce.getArguments().size(); i += 2) {
                ti = (IAType) env.getType(fce.getArguments().get(i).getValue());
                tagi = ti.getTypeTag();
                if (ti.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) ti))
                    tagi = ((AUnionType) ti).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                            .getTypeTag();
                if (tag0 != tagi)
                    throw new AlgebricksException(errMsg3);
            }
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        return t1;
    }
}
