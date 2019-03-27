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

import java.util.HashMap;

import org.apache.asterix.lexergenerator.rulegenerators.RuleGenerator;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorAnythingUntil;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorCaseInsensitiveChar;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorChar;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorCharOrNothing;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorDigitSequence;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorNothing;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorSign;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorSignOrNothing;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorString;
import org.apache.asterix.lexergenerator.rulegenerators.RuleGeneratorToken;

public class NodeChainFactory {
    static private HashMap<String, RuleGenerator> ruleGenerators = new HashMap<String, RuleGenerator>();

    static {
        ruleGenerators.put("char", new RuleGeneratorChar());
        ruleGenerators.put("string", new RuleGeneratorString());
        ruleGenerators.put("anythingUntil", new RuleGeneratorAnythingUntil());
        ruleGenerators.put("signOrNothing", new RuleGeneratorSignOrNothing());
        ruleGenerators.put("sign", new RuleGeneratorSign());
        ruleGenerators.put("digitSequence", new RuleGeneratorDigitSequence());
        ruleGenerators.put("caseInsensitiveChar", new RuleGeneratorCaseInsensitiveChar());
        ruleGenerators.put("charOrNothing", new RuleGeneratorCharOrNothing());
        ruleGenerators.put("token", new RuleGeneratorToken());
        ruleGenerators.put("nothing", new RuleGeneratorNothing());
    }

    public static LexerNode create(String generator, String constructor) throws Exception {
        constructor = constructor.replace("@", "aux_");
        if (ruleGenerators.get(generator) == null)
            throw new Exception("Rule Generator not found for '" + generator + "'");
        return ruleGenerators.get(generator).generate(constructor);
    }
}
