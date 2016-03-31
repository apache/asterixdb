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
package org.apache.asterix.lexergenerator.rules;

public class RuleChar implements Rule {

    private char expected;

    public RuleChar clone() {
        return new RuleChar(expected);
    }

    public RuleChar(char expected) {
        this.expected = expected;
    }

    @Override
    public String toString() {
        return String.valueOf(expected);
    }

    public char expectedChar() {
        return expected;
    }

    @Override
    public int hashCode() {
        return (int) expected;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o instanceof RuleChar) {
            if (((RuleChar) o).expected == this.expected) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String javaAction() {
        return "currentChar = readNextChar();";
    }

    @Override
    public String javaMatch(String action) {
        StringBuilder result = new StringBuilder();
        result.append("if (currentChar=='");
        result.append(expected);
        result.append("'){");
        result.append(action);
        result.append("}");
        return result.toString();
    }
}
