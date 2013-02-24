package edu.uci.ics.asterix.lexergenerator;

import edu.uci.ics.asterix.lexergenerator.rules.Rule;
import edu.uci.ics.asterix.lexergenerator.rules.RuleChar;

public class Fixtures {
    static  String token_name       = "MYTOKEN";
    static  String token2_name       = "MYTOKEN2";
    static  String token_return     = "return TOKEN_MYTOKEN;\n";
    static  String token2_return     = "return TOKEN_MYTOKEN2;\n";
    static  String token_parseerror = "return parseError(TOKEN_MYTOKEN);\n";
    static  String token_tostring   = "! ";
    static  String rule_action      = "myaction";
    static  String rule_name        = "myrule";
    static  String rule_match       = "matchCheck("+rule_name+")";
    static  String rule2_action     = "myaction2";
    static  String rule2_name       = "myrule2";
    static  String rule2_match      = "matchCheck2("+rule_name+")";
    
    static public Rule createRule(final String name){
        return new Rule(){
            String rule_name        = name;
            String rule_action      = "myaction";
            String rule_match       = "matchCheck("+rule_name+")";
            
            @Override
            public Rule clone(){
                return Fixtures.createRule(name+"_clone");
            }
            
            @Override
            public String javaAction() {
                return rule_action;
            }

            @Override
            public String javaMatch(String action) {
                return rule_match+"{"+action+"}";
            }
            
            @Override
            public String toString(){
                return rule_name;
            }
            
        }; 
    }
    
    static Rule rule = new Rule(){
        
        public Rule clone(){
            return null;
        }
        
        @Override
        public String javaAction() {
            return rule_action;
        }

        @Override
        public String javaMatch(String action) {
            return rule_match+"{"+action+"}";
        }
        
        @Override
        public String toString(){
            return rule_name;
        }
        
    }; 

    static Rule rule2 = new Rule(){

        public Rule clone(){
            return null;
        }
        
        @Override
        public String javaAction() {
            return rule2_action;
        }

        @Override
        public String javaMatch(String act) {
            return rule2_match+"{"+act+"}";
        }
        
        @Override
        public String toString(){
            return rule2_name;
        }
        
    };
    
    static RuleChar ruleA = new RuleChar('a');
    static RuleChar ruleB = new RuleChar('b');
    static RuleChar ruleC = new RuleChar('c');
    static String ruleABC_action = "currentChar = readNextChar();";
    
}
