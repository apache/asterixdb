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
package org.apache.asterix.om.typecomputer.impl;

import java.util.EnumSet;
import java.util.Set;

import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class StringIntToStringTypeComputer extends AbstractResultTypeComputer {
    public static final StringIntToStringTypeComputer INSTANCE = new StringIntToStringTypeComputer(0, 0, 1, 1);

    public static final StringIntToStringTypeComputer INSTANCE_TRIPLE_STRING =
            new StringIntToStringTypeComputer(0, 2, 3, 3);

    public static final StringIntToStringTypeComputer INSTANCE_STRING_REGEXP_REPLACE_WITH_FLAG =
            new StringIntToStringTypeComputer(0, 3, 3, 3);

    private final int stringArgIdxMin;

    private final int stringArgIdxMax;

    private final int intArgIdxMin;

    private final int intArgIdxMax;

    public StringIntToStringTypeComputer(int stringArgIdxMin, int stringArgIdxMax, int intArgIdxMin, int intArgIdxMax) {
        this.stringArgIdxMin = stringArgIdxMin;
        this.stringArgIdxMax = stringArgIdxMax;
        this.intArgIdxMin = intArgIdxMin;
        this.intArgIdxMax = intArgIdxMax;
    }

    @Override
    public void checkArgType(String funcName, int argIndex, IAType type, SourceLocation sourceLoc)
            throws AlgebricksException {
        ATypeTag tag = type.getTypeTag();
        boolean expectedStringType = false;
        if (stringArgIdxMin <= argIndex && argIndex <= stringArgIdxMax) {
            if (tag == ATypeTag.STRING) {
                return;
            }
            expectedStringType = true;
        }

        boolean expectedIntType = false;
        if (intArgIdxMin <= argIndex && argIndex <= intArgIdxMax) {
            switch (tag) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    return;
            }
            expectedIntType = true;
        }

        throw new TypeMismatchException(sourceLoc, funcName, argIndex, tag,
                getExpectedTypes(expectedStringType, expectedIntType));
    }

    @Override
    public IAType getResultType(ILogicalExpression expr, IAType... types) throws AlgebricksException {
        return BuiltinType.ASTRING;
    }

    private ATypeTag[] getExpectedTypes(boolean expectedStringType, boolean expectedIntType) {
        Set<ATypeTag> expectedTypes = EnumSet.noneOf(ATypeTag.class);
        if (expectedStringType) {
            expectedTypes.add(ATypeTag.STRING);
        }
        if (expectedIntType) {
            expectedTypes.add(ATypeTag.TINYINT);
            expectedTypes.add(ATypeTag.SMALLINT);
            expectedTypes.add(ATypeTag.INTEGER);
            expectedTypes.add(ATypeTag.BIGINT);
        }
        return expectedTypes.toArray(new ATypeTag[0]);
    }
}
