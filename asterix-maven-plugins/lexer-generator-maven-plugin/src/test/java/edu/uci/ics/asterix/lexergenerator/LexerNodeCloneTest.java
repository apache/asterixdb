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

import static edu.uci.ics.asterix.lexergenerator.Fixtures.*;
import static org.junit.Assert.*;

import org.junit.Test;

import edu.uci.ics.asterix.lexergenerator.LexerNode;

public class LexerNodeCloneTest {
    
    @Test
    public void Depth1() throws Exception {
        LexerNode node = new LexerNode();
        LexerNode newNode = node.clone();
        assertFalse(node == newNode);
    }
    
    
    @Test
    public void Depth2() throws Exception {
        LexerNode node = new LexerNode();
        node.append(createRule("my1"));
        node.add(createRule("my2"));
        node.add(ruleA);
        node.appendTokenName(token_name);
        LexerNode newNode = node.clone();

        assertEquals(" ( my1!  || my2!  || a!  ) ", node.toString());
        assertEquals(" ( my1_clone!  || my2_clone!  || a!  ) ", newNode.toString());
    }

    @Test
    public void Depth3() throws Exception {
        LexerNode node = new LexerNode();
        node.append(createRule("my1"));
        node.add(createRule("my2"));
        node.add(ruleA);
        node.appendTokenName(token_name);
        LexerNode node2 = new LexerNode();
        node2.append(createRule("my3"));
        node2.add(createRule("my4"));
        node2.add(ruleB);
        node2.appendTokenName(token2_name);
        node.append(node2);
        LexerNode newNode = node.clone();
        // TODO
        // assertEquals(" ( my1!  (  || my3_clone!  || my4_clone!  || b!  ) " +
        //		     " || my2!  (  || my3_clone!  || my4_clone!  || b!  ) " +
        //		     " || a!  (  || my3_clone!  || my4_clone!  || b!  )  ) ", node.toString());
        // assertEquals(" ( my1_clone!  (  || my3_clone_clone!  || my4_clone_clone!  || b!  ) " +
        //		     " || my2_clone!  (  || my3_clone_clone!  || my4_clone_clone!  || b!  ) " +
        //		     " || a!  (  || my3_clone_clone!  || my4_clone_clone!  || b!  )  ) ", newNode.toString());
    }
    
}
