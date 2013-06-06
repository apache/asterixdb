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

import java.util.HashMap;

import edu.uci.ics.asterix.lexergenerator.rulegenerators.*;

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
