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
