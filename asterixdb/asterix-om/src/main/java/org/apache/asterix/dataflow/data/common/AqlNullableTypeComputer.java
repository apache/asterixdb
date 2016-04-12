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
package org.apache.asterix.dataflow.data.common;

import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeHelper;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;

public class AqlNullableTypeComputer implements INullableTypeComputer {

    public static final AqlNullableTypeComputer INSTANCE = new AqlNullableTypeComputer();

    private AqlNullableTypeComputer() {
    }

    @Override
    public IAType makeNullableType(Object type) throws AlgebricksException {
        IAType t = (IAType) type;
        if (TypeHelper.canBeNull(t)) {
            return t;
        } else {
            return AUnionType.createNullableType(t);
        }
    }

    @Override
    public boolean canBeNull(Object type) {
        IAType t = (IAType) type;
        return TypeHelper.canBeNull(t);
    }

    @Override
    public Object getNonOptionalType(Object type) {
        IAType t = (IAType) type;
        return TypeHelper.getNonOptionalType(t);
    }
}
