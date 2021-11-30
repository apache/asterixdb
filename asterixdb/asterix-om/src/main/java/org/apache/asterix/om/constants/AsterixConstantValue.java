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
package org.apache.asterix.om.constants;

import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AMissing;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.IAObject;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class AsterixConstantValue implements IAlgebricksConstantValue {

    private final IAObject object;

    public AsterixConstantValue(IAObject object) {
        this.object = object;
    }

    @Override
    public boolean isFalse() {
        return object == ABoolean.FALSE;
    }

    @Override
    public boolean isMissing() {
        return object == AMissing.MISSING;
    }

    @Override
    public boolean isNull() {
        return object == ANull.NULL;
    }

    @Override
    public boolean isTrue() {
        return object == ABoolean.TRUE;
    }

    public IAObject getObject() {
        return object;
    }

    @Override
    public String toString() {
        return object.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AsterixConstantValue) {
            AsterixConstantValue v2 = (AsterixConstantValue) o;
            return object.deepEqual(v2.getObject());
        } else if (o instanceof IAlgebricksConstantValue) {
            return o.equals(this);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        switch (object.getType().getTypeTag()) {
            case MISSING:
                return ConstantExpression.MISSING.hashCode();
            case NULL:
                return ConstantExpression.NULL.hashCode();
            case BOOLEAN:
                return (isTrue() ? ConstantExpression.TRUE : ConstantExpression.FALSE).hashCode();
            default:
                return object.hash();
        }
    }
}
