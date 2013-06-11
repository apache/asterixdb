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
package edu.uci.ics.asterix.optimizer.rules;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.rewriter.rules.InlineVariablesRule;

public class AsterixInlineVariablesRule extends InlineVariablesRule {
    
    public AsterixInlineVariablesRule() {
        // Do not inline field accesses because doing so would interfere with our access method rewrites.
        // TODO: For now we must also exclude record constructor functions to avoid breaking our type casting rules
        // IntroduceStaticTypeCastRule and IntroduceDynamicTypeCastRule. 
        doNotInlineFuncs.add(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME);
        doNotInlineFuncs.add(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX);
        doNotInlineFuncs.add(AsterixBuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR);
        doNotInlineFuncs.add(AsterixBuiltinFunctions.OPEN_RECORD_CONSTRUCTOR);
    }
}
