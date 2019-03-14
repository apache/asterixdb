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
/*
 * Numeric round half to even
 * Author : Xiaoyu Ma@UC Irvine
 * 01/30/2012
 */
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class NumericRound2TypeComputer extends AbstractResultTypeComputer {

    public static final NumericRound2TypeComputer INSTANCE = new NumericRound2TypeComputer();

    private NumericRound2TypeComputer() {

    }

    @Override
    protected void checkArgType(FunctionIdentifier funcId, int argIndex, IAType type, SourceLocation sourceLoc)
            throws AlgebricksException {
        ATypeTag tag = type.getTypeTag();
        if (argIndex == 0) {
            switch (tag) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                    break;
                default:
                    throw new TypeMismatchException(sourceLoc, funcId, argIndex, tag, ATypeTag.TINYINT,
                            ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT, ATypeTag.FLOAT, ATypeTag.DOUBLE);
            }
        }
        if (argIndex == 1) {
            switch (tag) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    break;
                default:
                    throw new TypeMismatchException(sourceLoc, funcId, argIndex, tag, ATypeTag.TINYINT,
                            ATypeTag.SMALLINT, ATypeTag.INTEGER, ATypeTag.BIGINT);
            }
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        ATypeTag tag = strippedInputTypes[0].getTypeTag();
        switch (tag) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case ANY:
                return strippedInputTypes[0];
            default:
                return null;
        }
    }
}
