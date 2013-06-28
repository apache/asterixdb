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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.asterix.lexergenerator.rules.*;

public class LexerNode {
    private static String TOKEN_PREFIX = "TOKEN_";
    private LinkedHashMap<Rule, LexerNode> actions = new LinkedHashMap<Rule, LexerNode>();
    private String finalTokenName;
    private Set<String> ongoingParsing = new HashSet<String>();

    public LexerNode clone() {
        LexerNode node = new LexerNode();
        node.finalTokenName = this.finalTokenName;
        for (Map.Entry<Rule, LexerNode> entry : this.actions.entrySet()) {
            node.actions.put(entry.getKey().clone(), entry.getValue().clone());
        }
        for (String ongoing : this.ongoingParsing) {
            node.ongoingParsing.add(ongoing);
        }
        return node;
    }

    public void add(Rule newRule) {
        if (actions.get(newRule) == null) {
            actions.put(newRule, new LexerNode());
        }
    }

    public void append(Rule newRule) {
        if (actions.size() == 0) {
            add(newRule);
        } else {
            for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
                action.getValue().append(newRule);
            }
            if (actions.containsKey(new RuleEpsilon())) {
                actions.remove(new RuleEpsilon());
                add(newRule);
            }
        }
    }

    public void merge(LexerNode newNode) throws Exception {
        for (Map.Entry<Rule, LexerNode> action : newNode.actions.entrySet()) {
            if (this.actions.get(action.getKey()) == null) {
                this.actions.put(action.getKey(), action.getValue());
            } else {
                this.actions.get(action.getKey()).merge(action.getValue());
            }
        }
        if (newNode.finalTokenName != null) {
            if (this.finalTokenName == null) {
                this.finalTokenName = newNode.finalTokenName;
            } else {
                throw new Exception("Rule conflict between: " + this.finalTokenName + " and " + newNode.finalTokenName);
            }
        }
        for (String ongoing : newNode.ongoingParsing) {
            this.ongoingParsing.add(ongoing);
        }
    }

    public void append(LexerNode node) throws Exception {
        for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
            if (action.getKey() instanceof RuleEpsilon)
                continue;
            action.getValue().append(node);
        }
        if (actions.containsKey(new RuleEpsilon())) {
            actions.remove(new RuleEpsilon());
            merge(node.clone());
        }
        if (actions.size() == 0 || finalTokenName != null) {
            finalTokenName = null;
            merge(node.clone());
        }
    }

    public void appendTokenName(String name) {
        if (actions.size() == 0) {
            this.finalTokenName = name;
        } else {
            ongoingParsing.add(TOKEN_PREFIX + name);
            for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
                action.getValue().appendTokenName(name);
            }
        }
    }

    public LexerNode removeTokensName() {
        this.finalTokenName = null;
        this.ongoingParsing.clear();
        for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
            action.getValue().removeTokensName();
        }
        return this;
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        if (finalTokenName != null)
            result.append("! ");
        if (actions.size() == 1)
            result.append(actions.keySet().toArray()[0].toString() + actions.values().toArray()[0].toString());
        if (actions.size() > 1) {
            result.append(" ( ");
            for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
                if (result.length() != 3) {
                    result.append(" || ");
                }
                result.append(action.getKey().toString());
                result.append(action.getValue().toString());
            }
            result.append(" ) ");
        }
        return result.toString();
    }

    public String toJava() {
        StringBuffer result = new StringBuffer();
        if (numberOfRuleChar() > 2) {
            result.append(toJavaSingleCharRules());
            result.append(toJavaComplexRules(false));
        } else {
            result.append(toJavaComplexRules(true));
        }
        if (this.finalTokenName != null) {
            result.append("return " + TOKEN_PREFIX + finalTokenName + ";\n");
        } else if (ongoingParsing != null) {
            String ongoingParsingArgs = collectionJoin(ongoingParsing, ',');
            result.append("return parseError(" + ongoingParsingArgs + ");\n");
        }
        return result.toString();
    }

    private int numberOfRuleChar() {
        int singleCharRules = 0;
        for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
            if (action.getKey() instanceof RuleChar)
                singleCharRules++;
        }
        return singleCharRules;
    }

    private String toJavaSingleCharRules() {
        StringBuffer result = new StringBuffer();
        result.append("switch(currentChar){\n");
        for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
            if (action.getKey() instanceof RuleChar) {
                RuleChar rule = (RuleChar) action.getKey();
                result.append("case '" + rule.expectedChar() + "':\n");
                result.append(rule.javaAction()).append("\n");
                result.append(action.getValue().toJava());
            }
        }
        result.append("}\n");
        return result.toString();
    }

    private String toJavaComplexRules(boolean all) {
        StringBuffer result = new StringBuffer();
        for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
            if (!all && action.getKey() instanceof RuleChar)
                continue;
            if (action.getKey() instanceof RuleEpsilon)
                continue;
            String act = action.getKey().javaAction();
            if (act.length() > 0) {
                act = "\n" + act;
            }
            result.append(action.getKey().javaMatch(act + "\n" + action.getValue().toJava()));
        }
        return result.toString();
    }

    public void expandFirstAction(LinkedHashMap<String, Token> tokens) throws Exception {
        for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
            Rule first = action.getKey();
            if (first instanceof RulePartial) {
                if (tokens.get(((RulePartial) first).getPartial()) == null) {
                    throw new Exception("Cannot find a token used as part of another definition, missing token: "
                            + ((RulePartial) first).getPartial());
                }
                actions.remove(first);
                LexerNode node = tokens.get(((RulePartial) first).getPartial()).getNode().clone();
                merge(node);
            }
        }
    }

    public Set<String> neededAuxFunctions() {
        HashSet<String> partials = new HashSet<String>();
        for (Map.Entry<Rule, LexerNode> action : actions.entrySet()) {
            Rule rule = action.getKey();
            if (rule instanceof RulePartial) {
                partials.add(((RulePartial) rule).getPartial());
            }
            partials.addAll(action.getValue().neededAuxFunctions());
        }
        return partials;
    }

    public String toJavaAuxFunction() {
        String oldFinalTokenName = finalTokenName;
        if (oldFinalTokenName == null)
            finalTokenName = "AUX_NOT_FOUND";
        String result = toJava();
        finalTokenName = oldFinalTokenName;
        return result;
    }

    private String collectionJoin(Collection<String> collection, char c) {
        StringBuilder ongoingParsingArgs = new StringBuilder();
        for (String token : collection) {
            ongoingParsingArgs.append(token);
            ongoingParsingArgs.append(c);
        }
        if (ongoingParsing.size() > 0) {
            ongoingParsingArgs.deleteCharAt(ongoingParsingArgs.length() - 1);
        }
        return ongoingParsingArgs.toString();
    }
}
