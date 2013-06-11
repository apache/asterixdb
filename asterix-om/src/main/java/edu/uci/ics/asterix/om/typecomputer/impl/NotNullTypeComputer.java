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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * This class is the type computer for not-null function.
 * If the input type is not a union, we just return it.
 * If the input type is a union,
 * case 1: we return a new union without null if the new union still has more than one types;
 * case 2: we return the non-null item type in the original union if there are only null and it in the original union.
 */
public class NotNullTypeComputer implements IResultTypeComputer {

    public static final NotNullTypeComputer INSTANCE = new NotNullTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        IAType type = (IAType) env.getType(f.getArguments().get(0).getValue());
        if (type.getTypeTag() != ATypeTag.UNION) {
            // directly return the input type if it is not a union
            return type;
        }

        AUnionType unionType = (AUnionType) type;
        List<IAType> items = new ArrayList<IAType>();
        // copy the item types
        items.addAll(unionType.getUnionList());

        // remove null
        for (int i = items.size() - 1; i >= 0; i--) {
            IAType itemType = items.get(i);
            if (itemType.getTypeTag() == ATypeTag.NULL) {
                items.remove(i);
            }
        }
        if (items.size() == 1) {
            //only one type is left
            return items.get(0);
        } else {
            //more than two types are left
            return new AUnionType(items, unionType.getTypeName());
        }
    }
}
