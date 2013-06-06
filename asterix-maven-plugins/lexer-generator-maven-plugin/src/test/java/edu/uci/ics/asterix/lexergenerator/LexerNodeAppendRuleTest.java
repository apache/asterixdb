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
package edu.uci.ics.asterix.lexergenerator;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.uci.ics.asterix.lexergenerator.LexerNode;
import static edu.uci.ics.asterix.lexergenerator.Fixtures.*;

public class LexerNodeAppendRuleTest {    
    @Test
    public void SingleNode() {
        LexerNode node = new LexerNode();
        node.appendTokenName(token_name);
        assertEquals(token_tostring, node.toString());
        assertEquals(token_return,   node.toJava());
    }

    @Test
    public void NodeRuleNode() {
        LexerNode node = new LexerNode();
        node.append(rule);
        node.appendTokenName(token_name);
        assertEquals(rule_name+token_tostring, node.toString());
        assertEquals(rule_match+"{"
                        +"\n"+rule_action
                        +"\n"+token_return
                     +"}"+token_parseerror, node.toJava());
    }

    @Test
    public void NodeRuleNodeRuleNode() {
        LexerNode node = new LexerNode();
        node.append(rule);
        node.append(rule2);
        node.appendTokenName(token_name);
        assertEquals(rule_name+rule2_name+token_tostring, node.toString());
        assertEquals(rule_match+"{"
                        +"\n"+rule_action
                        +"\n"+rule2_match+"{"
                            +"\n"+rule2_action
                            +"\n"+token_return
                        +"}"
                        +token_parseerror
                     +"}"+token_parseerror, node.toJava());
    }
}