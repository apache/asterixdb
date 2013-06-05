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
package edu.uci.ics.asterix.lexergenerator.rulegenerators;

import edu.uci.ics.asterix.lexergenerator.LexerNode;
import edu.uci.ics.asterix.lexergenerator.rules.RulePartial;

public class RuleGeneratorToken implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        if (input == null || input.length() == 0)
            throw new Exception("Wrong rule format for generator token : " + input);
        LexerNode node = new LexerNode();
        node.add(new RulePartial(input));
        return node;
    }

}
