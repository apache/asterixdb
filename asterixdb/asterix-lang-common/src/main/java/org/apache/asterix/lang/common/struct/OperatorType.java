/*
3 * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.asterix.lang.common.struct;

public enum OperatorType {
    OR("or"),
    AND("and"),
    LT("<"),
    GT(">"),
    LE("<="),
    GE(">="),
    EQ("="),
    NEQ("!="),
    PLUS("+"),
    MINUS("-"),
    CONCAT("||"),
    MUL("*"),
    DIVIDE("/"),
    DIV("div"),
    MOD("mod"),
    CARET("^"),
    FUZZY_EQ("~="),
    LIKE("like"),
    NOT_LIKE("not_like"),
    IN("in"),
    NOT_IN("not_in"),
    BETWEEN("between"),
    NOT_BETWEEN("not_between");

    private static final OperatorType[] VALUES = values();

    private final String symbol;

    OperatorType(String s) {
        symbol = s;
    }

    @Override
    public String toString() {
        return symbol;
    }

    public static OperatorType fromSymbol(String symbol) {
        for (OperatorType opType : VALUES) {
            if (opType.symbol.equals(symbol)) {
                return opType;
            }
        }
        return null;
    }
}
