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
package org.apache.asterix.app.function;

import org.apache.asterix.common.functions.FunctionConstants;
import org.apache.asterix.metadata.declared.FunctionDataSource;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

/**
 * This TPC-DS function is used to generate data with accordance to the specifications of the TPC Benchmark DS.
 *
 * This version of the function takes 1 argument:
 * - Scaling factor that decides the data size to be generated.
 *
 * This function will generate the data for all the TPC-DS tables
 */

public class TPCDSAllTablesDataGeneratorRewriter extends FunctionRewriter {

    public static final FunctionIdentifier TPCDS_ALL_TABLES_DATA_GENERATOR =
            new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "tpcds-datagen", 1);
    public static final TPCDSAllTablesDataGeneratorRewriter INSTANCE =
            new TPCDSAllTablesDataGeneratorRewriter(TPCDS_ALL_TABLES_DATA_GENERATOR);

    private TPCDSAllTablesDataGeneratorRewriter(FunctionIdentifier functionId) {
        super(functionId);
    }

    @Override
    protected FunctionDataSource toDatasource(IOptimizationContext context, AbstractFunctionCallExpression function)
            throws AlgebricksException {

        UnnestingFunctionCallExpression functionCall = (UnnestingFunctionCallExpression) function;
        ConstantExpression scalingFactorArgument = (ConstantExpression) functionCall.getArguments().get(0).getValue();

        // Extract the values
        IAObject scalingFactorArgumentValue = ((AsterixConstantValue) scalingFactorArgument.getValue()).getObject();

        // Get the arguments' types and validate them
        IAType scalingFactorType = scalingFactorArgumentValue.getType();

        // Ensure the scaling factor can be promoted to double
        if (!ATypeHierarchy.canPromote(scalingFactorType.getTypeTag(), ATypeTag.DOUBLE)) {
            throw new TypeMismatchException(getFunctionIdentifier(), 1, scalingFactorType.getTypeTag(),
                    ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT,
                    ATypeTag.DOUBLE);
        }

        // Convert whichever number type we received into double
        double scalingFactor = getScalingFactor(scalingFactorArgumentValue);

        return new TPCDSAllTablesDataGeneratorDatasource(context.getComputationNodeDomain(), scalingFactor,
                getFunctionIdentifier());
    }

    /**
     * Converts whichever received number type (byte, int, float, ...) into a double value
     *
     * @param value IAObject containing the numerical value
     *
     * @return The double value of the IAObject
     */
    private double getScalingFactor(IAObject value) throws TypeMismatchException {
        switch (value.getType().getTypeTag()) {
            case TINYINT:
                return ((AInt8) value).getByteValue();
            case SMALLINT:
                return ((AInt16) value).getShortValue();
            case INTEGER:
                return ((AInt32) value).getIntegerValue();
            case BIGINT:
                return ((AInt64) value).getLongValue();
            case FLOAT:
                return ((AFloat) value).getFloatValue();
            case DOUBLE:
                return ((ADouble) value).getDoubleValue();
            default:
                throw new TypeMismatchException(getFunctionIdentifier(), 1, value.getType().getTypeTag(),
                        ATypeTag.TINYINT, ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT,
                        ATypeTag.DOUBLE);
        }
    }

    /**
     * Gets the function identifier
     *
     * @return function identifier
     */
    private FunctionIdentifier getFunctionIdentifier() {
        return functionId;
    }
}
