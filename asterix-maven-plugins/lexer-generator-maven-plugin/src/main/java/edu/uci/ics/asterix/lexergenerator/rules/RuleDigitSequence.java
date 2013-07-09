/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.lexergenerator.rules;

public class RuleDigitSequence implements Rule {

    public RuleDigitSequence clone() {
        return new RuleDigitSequence();
    }

    @Override
    public String toString() {
        return " [0-9]+ ";
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o instanceof RuleDigitSequence) {
            return true;
        }
        return false;
    }

    @Override
    public String javaAction() {
        return "";
    }

    @Override
    public String javaMatch(String action) {
        StringBuilder result = new StringBuilder();
        result.append("if(currentChar >= '0' && currentChar<='9'){" + "\ncurrentChar = readNextChar();"
                + "\nwhile(currentChar >= '0' && currentChar<='9'){" + "\ncurrentChar = readNextChar();" + "\n}\n");
        result.append(action);
        result.append("\n}");
        return result.toString();
    }
}
