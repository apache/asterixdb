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

import org.apache.asterix.lang.common.base.Literal;

public class FalseLiteral extends Literal {

    private static final long serialVersionUID = -750814844423165149L;
    public static final FalseLiteral INSTANCE = new FalseLiteral();

    private FalseLiteral() {
    }

    @Override
    public Type getLiteralType() {
        return Type.FALSE;
    }

    @Override
    public String getStringValue() {
        return "false";
    }

    @Override
    public String toString() {
        return getStringValue();
    }

    @Override
    public Boolean getValue() {
        return Boolean.FALSE;
    }

    @Override
    public int hashCode() {
        return (int) serialVersionUID;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == INSTANCE;
    }

}
