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

import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

public class AStringTypeComputer extends AbstractConstructorTypeComputer {

    public static final AStringTypeComputer INSTANCE = new AStringTypeComputer(false);

    public static final AStringTypeComputer INSTANCE_NULLABLE = new AStringTypeComputer(true);

    private AStringTypeComputer(boolean nullable) {
        super(BuiltinType.ASTRING, nullable);
    }

    @Override
    protected boolean isAlwaysCastable(IAType inputType) {
        if (super.isAlwaysCastable(inputType)) {
            return true;
        }
        switch (inputType.getTypeTag()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case DATETIME:
            case DATE:
            case TIME:
            case DURATION:
            case YEARMONTHDURATION:
            case DAYTIMEDURATION:
            case UUID:
            case BINARY:
                return true;
            default:
                return false;
        }
    }
}
