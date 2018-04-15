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
package org.apache.asterix.lang.common.literal;

import java.util.Objects;

import org.apache.asterix.lang.common.base.Literal;

public class IntegerLiteral extends Literal {
    private static final long serialVersionUID = -8633520244871361967L;
    private Integer value;

    public IntegerLiteral(Integer value) {
        super();
        this.value = value;
    }

    @Override
    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    @Override
    public Type getLiteralType() {
        return Type.INTEGER;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof IntegerLiteral)) {
            return false;
        }
        IntegerLiteral target = (IntegerLiteral) object;
        return value.equals(target.getValue());
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
