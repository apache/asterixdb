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

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;

public class IfMissingOrNullTypeComputer extends AbstractIfMissingOrNullTypeComputer {

    public static final IResultTypeComputer INSTANCE = new IfMissingOrNullTypeComputer();

    @Override
    protected boolean equalsIfType(ATypeTag typeTag) {
        return typeTag == ATypeTag.MISSING || typeTag == ATypeTag.NULL;
    }

    @Override
    protected boolean intersectsIfType(AUnionType type) {
        return type.isUnknownableType();
    }

    @Override
    protected ATypeTag getOutputQuantifier(AUnionType type) {
        // 'missing' and 'null' cannot be in the output
        return null;
    }
}
