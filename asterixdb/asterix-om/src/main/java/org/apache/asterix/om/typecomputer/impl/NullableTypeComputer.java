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

import static org.apache.asterix.om.types.BuiltinType.ABINARY;
import static org.apache.asterix.om.types.BuiltinType.ABOOLEAN;
import static org.apache.asterix.om.types.BuiltinType.ADATE;
import static org.apache.asterix.om.types.BuiltinType.ADATETIME;
import static org.apache.asterix.om.types.BuiltinType.ADAYTIMEDURATION;
import static org.apache.asterix.om.types.BuiltinType.ADOUBLE;
import static org.apache.asterix.om.types.BuiltinType.ADURATION;
import static org.apache.asterix.om.types.BuiltinType.AFLOAT;
import static org.apache.asterix.om.types.BuiltinType.AINT16;
import static org.apache.asterix.om.types.BuiltinType.AINT32;
import static org.apache.asterix.om.types.BuiltinType.AINT64;
import static org.apache.asterix.om.types.BuiltinType.AINT8;
import static org.apache.asterix.om.types.BuiltinType.ASTRING;
import static org.apache.asterix.om.types.BuiltinType.ATIME;
import static org.apache.asterix.om.types.BuiltinType.AUUID;
import static org.apache.asterix.om.types.BuiltinType.AYEARMONTHDURATION;

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NullableTypeComputer implements IResultTypeComputer {

    public static final NullableTypeComputer INSTANCE_INT8 = new NullableTypeComputer(AINT8);
    public static final NullableTypeComputer INSTANCE_INT16 = new NullableTypeComputer(AINT16);
    public static final NullableTypeComputer INSTANCE_INT32 = new NullableTypeComputer(AINT32);
    public static final NullableTypeComputer INSTANCE_INT64 = new NullableTypeComputer(AINT64);
    public static final NullableTypeComputer INSTANCE_FLOAT = new NullableTypeComputer(AFLOAT);
    public static final NullableTypeComputer INSTANCE_DOUBLE = new NullableTypeComputer(ADOUBLE);
    public static final NullableTypeComputer INSTANCE_BOOLEAN = new NullableTypeComputer(ABOOLEAN);
    public static final NullableTypeComputer INSTANCE_STRING = new NullableTypeComputer(ASTRING);
    public static final NullableTypeComputer INSTANCE_DATE = new NullableTypeComputer(ADATE);
    public static final NullableTypeComputer INSTANCE_TIME = new NullableTypeComputer(ATIME);
    public static final NullableTypeComputer INSTANCE_DATE_TIME = new NullableTypeComputer(ADATETIME);
    public static final NullableTypeComputer INSTANCE_DURATION = new NullableTypeComputer(ADURATION);
    public static final NullableTypeComputer INSTANCE_DAY_TIME_DURATION = new NullableTypeComputer(ADAYTIMEDURATION);
    public static final NullableTypeComputer INSTANCE_YEAR_MONTH_DURATION =
            new NullableTypeComputer(AYEARMONTHDURATION);
    public static final NullableTypeComputer INSTANCE_UUID = new NullableTypeComputer(AUUID);
    public static final NullableTypeComputer INSTANCE_BINARY = new NullableTypeComputer(ABINARY);

    private final IAType nullablePrimeType;

    private NullableTypeComputer(IAType primeType) {
        this.nullablePrimeType = AUnionType.createNullableType(primeType);
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        return nullablePrimeType;
    }
}
