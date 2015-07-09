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
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class SubsetCollectionTypeComputer implements IResultTypeComputer {

    public static final SubsetCollectionTypeComputer INSTANCE = new SubsetCollectionTypeComputer();

    private SubsetCollectionTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env, IMetadataProvider<?, ?> mp)
            throws AlgebricksException {
        AbstractFunctionCallExpression fun = (AbstractFunctionCallExpression) expression;
        IAType t;
        try {
            t = (IAType) env.getType(fun.getArguments().get(0).getValue());
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        switch (t.getTypeTag()) {
            case UNORDEREDLIST:
            case ORDEREDLIST: {
                AbstractCollectionType act = (AbstractCollectionType) t;
                return act.getItemType();
            }
            case UNION: {
                AUnionType ut = (AUnionType) t;
                if (!ut.isNullableType()) {
                    throw new AlgebricksException("Expecting collection type. Found " + t);
                }
                IAType t2 = ut.getUnionList().get(1);
                ATypeTag tag2 = t2.getTypeTag();
                if (tag2 == ATypeTag.UNORDEREDLIST || tag2 == ATypeTag.ORDEREDLIST) {
                    AbstractCollectionType act = (AbstractCollectionType) t2;
                    return act.getItemType();
                }
                throw new AlgebricksException("Expecting collection type. Found " + t);
            }
            default: {
                throw new AlgebricksException("Expecting collection type. Found " + t);
            }
        }
    }
}
