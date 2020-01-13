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

import static org.apache.asterix.om.types.ATypeTag.ANY;

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class NumericBinaryToDoubleTypeComputer extends AbstractResultTypeComputer {

    public static final NumericBinaryToDoubleTypeComputer INSTANCE = new NumericBinaryToDoubleTypeComputer();

    private NumericBinaryToDoubleTypeComputer() {
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        ATypeTag t0 = strippedInputTypes[0].getTypeTag();
        ATypeTag t1 = strippedInputTypes[1].getTypeTag();
        if (!isValidType(t0) || !isValidType(t1)) {
            return BuiltinType.ANULL;
        }
        return (t0 == ANY || t1 == ANY) ? AUnionType.createUnknownableType(BuiltinType.ADOUBLE) : BuiltinType.ADOUBLE;
    }

    private static boolean isValidType(ATypeTag tag) {
        return ATypeHierarchy.getTypeDomain(tag) == ATypeHierarchy.Domain.NUMERIC || tag == ANY;
    }
}
