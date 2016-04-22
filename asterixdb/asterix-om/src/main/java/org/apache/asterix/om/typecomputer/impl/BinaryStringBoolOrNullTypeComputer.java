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

import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

/**
 * @author Xiaoyu Ma
 */
public class BinaryStringBoolOrNullTypeComputer extends AbstractBinaryStringTypeComputer {
    public static final BinaryStringBoolOrNullTypeComputer INSTANCE = new BinaryStringBoolOrNullTypeComputer();

    private BinaryStringBoolOrNullTypeComputer() {
    }

    @Override
    public IAType getResultType(IAType t0, IAType t1, boolean nullable) {
        if (nullable) {
            return AUnionType.createNullableType(BuiltinType.ABOOLEAN);
        }
        return BuiltinType.ABOOLEAN;
    }
}
