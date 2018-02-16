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
package org.apache.asterix.om.types.hierachy;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy.TypeCastingMathFunctionType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class IntegerToInt64TypeConvertComputer extends AbstractIntegerTypeConvertComputer {

    private static final IntegerToInt64TypeConvertComputer INSTANCE = new IntegerToInt64TypeConvertComputer();

    private IntegerToInt64TypeConvertComputer() {
        super(true);
    }

    public static IntegerToInt64TypeConvertComputer getInstance() {
        return INSTANCE;
    }

    @Override
    public void convertType(byte[] data, int start, int length, DataOutput out) throws IOException {
        convertIntegerType(data, start, length, out, ATypeTag.BIGINT, 8);
    }

    @Override
    public IAObject convertType(IAObject sourceObject, TypeCastingMathFunctionType mathFunction)
            throws HyracksDataException {
        return convertIntegerType(sourceObject, ATypeTag.BIGINT);
    }
}
