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

import java.util.Objects;

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public abstract class AbstractConstructorTypeComputer extends AbstractResultTypeComputer {

    protected final IAType primeType;

    protected final boolean nullable;

    protected AbstractConstructorTypeComputer(IAType primeType, boolean nullable) {
        this.primeType = Objects.requireNonNull(primeType);
        this.nullable = nullable;
    }

    @Override
    protected final IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes)
            throws AlgebricksException {
        if (!nullable || (strippedInputTypes.length == 1 && isAlwaysCastable(strippedInputTypes[0]))) {
            return primeType;
        } else {
            return AUnionType.createNullableType(primeType);
        }
    }

    protected boolean isAlwaysCastable(IAType inputType) {
        return primeType.deepEqual(inputType);
    }
}
