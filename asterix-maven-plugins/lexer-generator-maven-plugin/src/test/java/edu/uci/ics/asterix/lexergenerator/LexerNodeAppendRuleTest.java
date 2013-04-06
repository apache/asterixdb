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