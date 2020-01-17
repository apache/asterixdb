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

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class NumericAddSubMulDivTypeComputer extends AbstractResultTypeComputer {
    /**
     * For those functions that do not return NULL if both arguments are not NULL
     */
    public static final NumericAddSubMulDivTypeComputer INSTANCE_MUL_POW =
            new NumericAddSubMulDivTypeComputer(false, false, false);

    /**
     * For those functions that may return NULL even if both arguments are not NULL (e.g. division by zero)
     */
    public static final NumericAddSubMulDivTypeComputer INSTANCE_DIV_MOD =
            new NumericAddSubMulDivTypeComputer(true, false, false);

    public static final NumericAddSubMulDivTypeComputer INSTANCE_ADD =
            new NumericAddSubMulDivTypeComputer(false, true, false);

    public static final NumericAddSubMulDivTypeComputer INSTANCE_SUB =
            new NumericAddSubMulDivTypeComputer(false, true, true);

    private final boolean nullable;
    private final boolean isMixedTemporalAllowed;
    private final boolean isSameTemporalAllowed;

    private NumericAddSubMulDivTypeComputer(boolean nullable, boolean isMixedTemporalAllowed,
            boolean isSameTemporalAllowed) {
        this.nullable = nullable;
        this.isMixedTemporalAllowed = isMixedTemporalAllowed;
        this.isSameTemporalAllowed = isSameTemporalAllowed;
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType t1 = strippedInputTypes[0];
        IAType t2 = strippedInputTypes[1];
        ATypeTag tag1 = t1.getTypeTag();
        ATypeTag tag2 = t2.getTypeTag();

        IAType type;
        switch (tag1) {
            case DOUBLE:
                switch (tag2) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                    case FLOAT:
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case FLOAT:
                switch (tag2) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case BIGINT:
                switch (tag2) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                        type = BuiltinType.AINT64;
                        break;
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case INTEGER:
                switch (tag2) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                        type = BuiltinType.AINT32;
                        break;
                    case BIGINT:
                        type = BuiltinType.AINT64;
                        break;
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case SMALLINT:
                switch (tag2) {
                    case TINYINT:
                    case SMALLINT:
                        type = BuiltinType.AINT16;
                        break;
                    case INTEGER:
                        type = BuiltinType.AINT32;
                        break;
                    case BIGINT:
                        type = BuiltinType.AINT64;
                        break;
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case TINYINT:
                switch (tag2) {
                    case TINYINT:
                        type = BuiltinType.AINT8;
                        break;
                    case SMALLINT:
                        type = BuiltinType.AINT16;
                        break;
                    case INTEGER:
                        type = BuiltinType.AINT32;
                        break;
                    case BIGINT:
                        type = BuiltinType.AINT64;
                        break;
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case ANY:
                switch (tag2) {
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                    case FLOAT:
                    case DOUBLE:
                    case DATE:
                    case TIME:
                    case DATETIME:
                    case DURATION:
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case DATE:
                switch (tag2) {
                    case DATE:
                        if (!isSameTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADATE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case TIME:
                switch (tag2) {
                    case TIME:
                        if (!isSameTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                        return BuiltinType.ANULL;
                    case DAYTIMEDURATION:
                    case DURATION:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        // TODO(ali): check the runtime consistency of DURATION op TIME vs. TIME op DURATION
                        type = BuiltinType.ATIME;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case DATETIME:
                switch (tag2) {
                    case DATETIME:
                        if (!isSameTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADATETIME;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case DURATION:
                switch (tag2) {
                    case DATE:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADATE;
                        break;
                    case TIME:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        // TODO(ali): check the runtime consistency of DURATION op TIME and TIME op DURATION
                        // depending on the runtime values the operation might be invalid and produce NULL
                        type = AUnionType.createNullableType(BuiltinType.ATIME);
                        break;
                    case DATETIME:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADATETIME;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case YEARMONTHDURATION:
                switch (tag2) {
                    case DATE:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADATE;
                        break;
                    case TIME:
                        return BuiltinType.ANULL;
                    case DATETIME:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADATETIME;
                        break;
                    case YEARMONTHDURATION:
                        if (!isSameTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.AYEARMONTHDURATION;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            case DAYTIMEDURATION:
                switch (tag2) {
                    case DATE:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADATE;
                        break;
                    case TIME:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ATIME;
                        break;
                    case DATETIME:
                        if (!isMixedTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADATETIME;
                        break;
                    case DAYTIMEDURATION:
                        if (!isSameTemporalAllowed) {
                            return BuiltinType.ANULL;
                        }
                        type = BuiltinType.ADAYTIMEDURATION;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        return BuiltinType.ANULL;
                }
                break;
            default:
                return BuiltinType.ANULL;
        }

        if (nullable && type.getTypeTag() != ATypeTag.ANY) {
            type = AUnionType.createNullableType(type);
        }

        return type;
    }
}
