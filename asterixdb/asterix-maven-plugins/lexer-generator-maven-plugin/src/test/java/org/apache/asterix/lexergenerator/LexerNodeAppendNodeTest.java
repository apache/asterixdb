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

import org.apache.asterix.lexergenerator.rules.RuleEpsilon;
import org.junit.Test;

public class LexerNodeAppendNodeTest {

    @Test
    public void AppendIsMergeIfNoActions() throws Exception {
        LexerNode node = new LexerNode();
        LexerNode node2 = new LexerNode();
        node2.append(createRule("rule"));
        node2.appendTokenName(token_name);
        node.append(node2);
        assertEquals("rule_clone! ", node.toString());
    }

    @Test
    public void AppendIsAppend() throws Exception {
        LexerNode node = new LexerNode();
        node.append(createRule("A"));
        LexerNode node2 = new LexerNode();
        node2.append(createRule("rule"));
        node2.appendTokenName(token_name);
        node.append(node2);
        assertEquals("Arule_clone! ", node.toString());
    }

    @Test
    public void AppendedNodesAreCloned() throws Exception {
        LexerNode node = new LexerNode();
        node.append(createRule("A"));
        node.appendTokenName(token_name);
        LexerNode node2 = new LexerNode();
        node2.append(createRule("B"));
        node2.appendTokenName(token2_name);
        node.append(node2);
        // TODO
        // assertEquals("A! B_clone! ", node.toString());

        LexerNode node3 = new LexerNode();
        node3.append(createRule("C"));
        node3.append(createRule("D"));
        node3.appendTokenName(token2_name);
        node.append(node3);
        // TODO
        // assertEquals("A! B_clone! C_cloneD_clone! ", node.toString());
    }

    @Test
    public void EpsilonRuleDoesNotPropagateAppended() throws Exception {
        LexerNode node = new LexerNode();
        node.append(new RuleEpsilon());
        LexerNode node2 = new LexerNode();
        node2.append(createRule("A"));
        node2.appendTokenName(token2_name);
        node.append(node2);
        assertEquals("A_clone! ", node.toString());
    }

    @Test
    public void EpsilonRuleIsRemovedAndIssueMerge() throws Exception {
        LexerNode node = new LexerNode();
        node.append(new RuleEpsilon());
        LexerNode node2 = new LexerNode();
        node2.append(createRule("A"));
        node2.appendTokenName(token2_name);
        node.append(node2);
        node.add(new RuleEpsilon());
        node.append(node2);
        // TODO
        // assertEquals(" ( A_clone! A_clone!  || A_clone!  ) ", node.toString());
    }

}
