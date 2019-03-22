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

import static org.apache.asterix.lexergenerator.Fixtures.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class LexerNodeAddRuleTest {

    @Test
    public void NodeRuleRuleNodeNode() {
        LexerNode node = new LexerNode();
        node.append(rule);
        node.add(rule2);
        node.appendTokenName(token_name);
        assertEquals(" ( " + rule_name + token_tostring + " || " + rule2_name + token_tostring + " ) ",
                node.toString());
        assertEquals(rule_match + "{" + "\n" + rule_action + "\n" + token_return + "}" + rule2_match + "{" + "\n"
                + rule2_action + "\n" + token_return + "}" + token_parseerror, node.toJava());
    }

    @Test
    public void NodeSwitchCase() {
        LexerNode node = new LexerNode();
        node.append(ruleA);
        node.add(ruleB);
        node.add(ruleC);
        node.appendTokenName(token_name);
        assertEquals(" ( a" + token_tostring + " || b" + token_tostring + " || c" + token_tostring + " ) ",
                node.toString());
        assertEquals("switch(currentChar){\n" + "case 'a':" + "\n" + ruleABC_action + "\n" + token_return + "case 'b':"
                + "\n" + ruleABC_action + "\n" + token_return + "case 'c':" + "\n" + ruleABC_action + "\n"
                + token_return + "}\n" + token_parseerror, node.toJava());
    }

}
