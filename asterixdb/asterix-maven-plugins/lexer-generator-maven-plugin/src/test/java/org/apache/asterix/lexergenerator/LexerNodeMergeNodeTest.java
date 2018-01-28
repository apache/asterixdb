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

import static org.apache.asterix.lexergenerator.Fixtures.rule;
import static org.apache.asterix.lexergenerator.Fixtures.rule2;
import static org.apache.asterix.lexergenerator.Fixtures.rule2_action;
import static org.apache.asterix.lexergenerator.Fixtures.rule2_match;
import static org.apache.asterix.lexergenerator.Fixtures.rule2_name;
import static org.apache.asterix.lexergenerator.Fixtures.rule_action;
import static org.apache.asterix.lexergenerator.Fixtures.rule_match;
import static org.apache.asterix.lexergenerator.Fixtures.rule_name;
import static org.apache.asterix.lexergenerator.Fixtures.token2_name;
import static org.apache.asterix.lexergenerator.Fixtures.token2_return;
import static org.apache.asterix.lexergenerator.Fixtures.token_name;
import static org.apache.asterix.lexergenerator.Fixtures.token_return;
import static org.apache.asterix.lexergenerator.Fixtures.token_tostring;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class LexerNodeMergeNodeTest {

    @Test
    public void MergeIsAdd() throws Exception {
        LexerNode node = new LexerNode();
        node.append(rule);
        LexerNode node2 = new LexerNode();
        node2.append(rule2);
        node2.append(rule);
        node2.merge(node);
        node2.appendTokenName(token_name);

        LexerNode expected = new LexerNode();
        expected.append(rule2);
        expected.append(rule);
        expected.add(rule);
        expected.appendTokenName(token_name);

        assertEquals(expected.toString(), node2.toString());
        assertEquals(expected.toJava(), node2.toJava());
    }

    @Test
    public void MergeTwoToken() throws Exception {
        LexerNode node = new LexerNode();
        node.append(rule);
        node.appendTokenName(token_name);
        LexerNode node2 = new LexerNode();
        node2.append(rule2);
        node2.appendTokenName(token2_name);
        node.merge(node2);

        assertEquals(" ( " + rule_name + token_tostring + " || " + rule2_name + token_tostring + " ) ",
                node.toString());
        assertEquals(
                rule_match + "{" + "\n" + rule_action + "\n" + token_return + "}" + rule2_match + "{" + "\n"
                        + rule2_action + "\n" + token2_return + "}return parseError(TOKEN_MYTOKEN,TOKEN_MYTOKEN2);\n",
                node.toJava());
    }

    @Test(expected = Exception.class)
    public void MergeConflict() throws Exception {
        LexerNode node = new LexerNode();
        node.append(rule);
        node.appendTokenName(token_name);
        LexerNode node2 = new LexerNode();
        node2.append(rule);
        node2.appendTokenName(token2_name);
        try {
            node.merge(node2);
        } catch (Exception e) {
            assertEquals("Rule conflict between: " + token_name + " and " + token2_name, e.getMessage());
            throw e;
        }
    }

    @Test
    public void MergeWithoutConflictWithRemoveTokensName() throws Exception {
        LexerNode node = new LexerNode();
        node.append(rule);
        node.append(rule);
        node.appendTokenName(token_name);
        LexerNode node2 = new LexerNode();
        node2.append(rule);
        node2.append(rule);
        node2.appendTokenName(token2_name);
        node2.removeTokensName();
        node.merge(node2);
        assertEquals(rule_name + rule_name + token_tostring, node.toString());
    }
}
