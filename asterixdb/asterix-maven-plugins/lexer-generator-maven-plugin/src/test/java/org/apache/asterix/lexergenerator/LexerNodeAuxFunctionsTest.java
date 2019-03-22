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

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

import org.apache.asterix.lexergenerator.rules.RuleEpsilon;
import org.apache.asterix.lexergenerator.rules.RulePartial;
import org.junit.Test;

public class LexerNodeAuxFunctionsTest {
    String expectedDifferentReturn = "return TOKEN_AUX_NOT_FOUND;\n";

    @Test
    public void NodeRuleRuleNodeNode() {
        LexerNode node = new LexerNode();
        node.append(rule);
        node.add(rule2);
        node.appendTokenName(token_name);
        assertEquals(" ( " + rule_name + token_tostring + " || " + rule2_name + token_tostring + " ) ",
                node.toString());
        assertEquals(rule_match + "{" + "\n" + rule_action + "\n" + token_return + "}" + rule2_match + "{" + "\n"
                + rule2_action + "\n" + token_return + "}" + expectedDifferentReturn, node.toJavaAuxFunction());
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
                + token_return + "}\n" + expectedDifferentReturn, node.toJavaAuxFunction());
    }

    @Test
    public void NodeNeededAuxFunctions() {
        LexerNode node = new LexerNode();
        node.append(ruleA);
        node.add(new RulePartial("token1"));
        node.append(ruleC);
        node.append(new RulePartial("token2"));
        node.appendTokenName(token_name);
        assertEquals(" ( actoken2!  || token1ctoken2!  ) ", node.toString());
        Set<String> expectedNeededAuxFunctions = new HashSet<String>();
        expectedNeededAuxFunctions.add("token1");
        expectedNeededAuxFunctions.add("token2");
        assertEquals(expectedNeededAuxFunctions, node.neededAuxFunctions());
    }

    @Test(expected = Exception.class)
    public void NodeExpandFirstActionError() throws Exception {
        LexerNode node = new LexerNode();
        node.append(ruleA);
        node.add(new RulePartial("token1"));
        node.append(ruleC);
        node.add(new RuleEpsilon());
        node.append(new RulePartial("token2"));
        node.appendTokenName(token_name);
        assertEquals(" ( actoken2!  || token1ctoken2!  || token2!  ) ", node.toString());
        LinkedHashMap<String, Token> tokens = new LinkedHashMap<String, Token>();
        try {
            node.expandFirstAction(tokens);
        } catch (Exception e) {
            assertEquals("Cannot find a token used as part of another definition, missing token: token1",
                    e.getMessage());
            throw e;
        }
    }

    public void NodeExpandFirstAction() throws Exception {
        LexerNode node = new LexerNode();
        node.append(ruleA);
        node.add(new RulePartial("token1"));
        node.append(ruleC);
        node.add(new RuleEpsilon());
        node.append(new RulePartial("token2"));
        node.appendTokenName(token_name);
        assertEquals(" ( actoken2!  || token1ctoken2!  || token2!  ) ", node.toString());
        LinkedHashMap<String, Token> tokens = new LinkedHashMap<String, Token>();
        Token a = new Token("token1 = string(T1-blabla)", tokens);
        Token b = new Token("token1 = string(T1-blabla)", tokens);
        tokens.put("token1", a);
        tokens.put("token2", b);
        node.expandFirstAction(tokens);
        assertEquals(" ( actoken2!  || T1-blablactoken2!  || T2-blabla!  ) ", node.toString());
    }
}
