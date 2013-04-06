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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.typecomputer.base.TypeComputerUtilities;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class OpenRecordConstructorResultType implements IResultTypeComputer {

    public static final OpenRecordConstructorResultType INSTANCE = new OpenRecordConstructorResultType();

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;

        /**
         * if type has been top-down propagated, use the enforced type
         */
        ARecordType type = (ARecordType) TypeComputerUtilities.getRequiredType(f);
        if (type != null)
            return type;

        int n = 0;
        Iterator<Mutable<ILogicalExpression>> argIter = f.getArguments().iterator();
        List<String> namesList = new ArrayList<String>();
        List<IAType> typesList = new ArrayList<IAType>();
        while (argIter.hasNext()) {
            ILogicalExpression e1 = argIter.next().getValue();
            ILogicalExpression e2 = argIter.next().getValue();
            IAType t2 = (IAType) env.getType(e2);
            if (e1.getExpressionTag() == LogicalExpressionTag.CONSTANT && t2 != null && TypeHelper.isClosed(t2)) {
                ConstantExpression nameExpr = (ConstantExpression) e1;
                AsterixConstantValue acv = (AsterixConstantValue) nameExpr.getValue();
                namesList.add(((AString) acv.getObject()).getStringValue());
                typesList.add(t2);
                n++;
            }
        }
        String[] fieldNames = new String[n];
        IAType[] fieldTypes = new IAType[n];
        fieldNames = namesList.toArray(fieldNames);
        fieldTypes = typesList.toArray(fieldTypes);
        try {
            return new ARecordType(null, fieldNames, fieldTypes, true);
        } catch (AsterixException e) {
            throw new AlgebricksException(e);
        }
    }
}
