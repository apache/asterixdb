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
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * This function is to make a sequence of records and a singleton collection of records
 * present in a uniformed manner.
 * 
 * @author yingyib
 */
public class CollectionToSequenceTypeComputer implements IResultTypeComputer {

    public static final CollectionToSequenceTypeComputer INSTANCE = new CollectionToSequenceTypeComputer();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression func = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg = func.getArguments().get(0).getValue();

        IAType argType = (IAType) env.getType(arg);
        if (TypeHelper.canBeNull(argType)) {
            IAType nonOptionalType = TypeHelper.getNonOptionalType(argType);
            if (nonOptionalType.getTypeTag() == ATypeTag.ORDEREDLIST
                    || nonOptionalType.getTypeTag() == ATypeTag.UNORDEREDLIST) {
                /** if the collection is null, that corresponds to an empty sequence */
                argType = nonOptionalType;
            }
        }

        ATypeTag argTypeTag = argType.getTypeTag();
        if (argTypeTag == ATypeTag.ORDEREDLIST || argTypeTag == ATypeTag.UNORDEREDLIST) {
            /** if the input is a singleton list, return it's item type if any */
            AbstractCollectionType collectionType = (AbstractCollectionType) argType;
            return collectionType.getItemType();
        } else {
            /** if the input is not a singleton list, return the original input type */
            return argType;
        }
    }

}
