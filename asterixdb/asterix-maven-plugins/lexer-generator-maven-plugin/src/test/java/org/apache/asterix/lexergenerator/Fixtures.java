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
package org.apache.asterix.lexergenerator;

import org.apache.asterix.lexergenerator.rules.Rule;
import org.apache.asterix.lexergenerator.rules.RuleChar;

public class Fixtures {
    static String token_name = "MYTOKEN";
    static String token2_name = "MYTOKEN2";
    static String token_return = "return TOKEN_MYTOKEN;\n";
    static String token2_return = "return TOKEN_MYTOKEN2;\n";
    static String token_parseerror = "return parseError(TOKEN_MYTOKEN);\n";
    static String token_tostring = "! ";
    static String rule_action = "myaction";
    static String rule_name = "myrule";
    static String rule_match = "matchCheck(" + rule_name + ")";
    static String rule2_action = "myaction2";
    static String rule2_name = "myrule2";
    static String rule2_match = "matchCheck2(" + rule_name + ")";

    static public Rule createRule(final String name) {
        return new Rule() {
            String rule_name = name;
            String rule_action = "myaction";
            String rule_match = "matchCheck(" + rule_name + ")";

            @Override
            public Rule clone() {
                return Fixtures.createRule(name + "_clone");
            }

            @Override
            public String javaAction() {
                return rule_action;
            }

            @Override
            public String javaMatch(String action) {
                return rule_match + "{" + action + "}";
            }

            @Override
            public String toString() {
                return rule_name;
            }

        };
    }

    static Rule rule = new Rule() {

        public Rule clone() {
            return null;
        }

        @Override
        public String javaAction() {
            return rule_action;
        }

        @Override
        public String javaMatch(String action) {
            return rule_match + "{" + action + "}";
        }

        @Override
        public String toString() {
            return rule_name;
        }

    };

    static Rule rule2 = new Rule() {

        public Rule clone() {
            return null;
        }

        @Override
        public String javaAction() {
            return rule2_action;
        }

        @Override
        public String javaMatch(String act) {
            return rule2_match + "{" + act + "}";
        }

        @Override
        public String toString() {
            return rule2_name;
        }

    };

    static RuleChar ruleA = new RuleChar('a');
    static RuleChar ruleB = new RuleChar('b');
    static RuleChar ruleC = new RuleChar('c');
    static String ruleABC_action = "currentChar = readNextChar();";

}
