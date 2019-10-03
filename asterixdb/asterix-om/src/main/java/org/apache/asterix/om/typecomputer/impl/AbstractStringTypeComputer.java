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
import org.apache.asterix.om.types.IAType;

/**
 * For function signature: nullable_return_type fun(string...)
 */
public abstract class AbstractStringTypeComputer extends AbstractResultTypeComputer {

    protected static IAType getType(IAType returnType, IAType... argsTypes) {
        // all args are expected to be strings. If any arg is not string (ANY or mismatched-type), return nullable
        for (IAType actualType : argsTypes) {
            if (actualType.getTypeTag() != ATypeTag.STRING) {
                return AUnionType.createNullableType(returnType);
            }
        }
        return returnType;
    }
}
