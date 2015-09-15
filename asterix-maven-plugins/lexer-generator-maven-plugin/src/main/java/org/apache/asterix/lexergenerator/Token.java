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

import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Token {
    private String userDescription;
    private String name;
    private LexerNode node;

    public Token(String str, LinkedHashMap<String, Token> tokens) throws Exception {
        userDescription = str;
        node = new LexerNode();
        parse(userDescription, tokens);
    }

    public String getName() {
        return name;
    }

    public LexerNode getNode() {
        return node;
    }

    public String toString() {
        return this.name + " => " + getNode().toString();
    }

    public void merge(Token newToken) throws Exception {
        node.merge(newToken.getNode());
    }

    private void parse(String str, LinkedHashMap<String, Token> tokens) throws Exception {
        Pattern p = Pattern.compile("^(@?\\w+)\\s*=\\s*(.+)");
        Matcher m = p.matcher(str);
        if (!m.find())
            throw new Exception("Token definition not correct: " + str);
        this.name = m.group(1).replaceAll("@", "aux_");
        String[] textRules = m.group(2).split("(?<!\\\\),\\s*");
        for (String textRule : textRules) {
            Pattern pRule = Pattern.compile("^(\\w+)(\\((.*)\\))?");
            Matcher mRule = pRule.matcher(textRule);
            mRule.find();
            String generator = mRule.group(1);
            String constructor = mRule.group(3);
            if (constructor == null)
                throw new Exception("Error in rule format: " + "\n " + str + " = " + generator + " : " + constructor);
            constructor = constructor.replace("\\", "");
            node.append(NodeChainFactory.create(generator, constructor));
            node.expandFirstAction(tokens);
        }
        node.appendTokenName(name);
    }

}
