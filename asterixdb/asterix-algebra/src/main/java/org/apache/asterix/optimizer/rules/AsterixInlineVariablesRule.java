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
package org.apache.asterix.optimizer.rules;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.hyracks.algebricks.rewriter.rules.InlineVariablesRule;

public class AsterixInlineVariablesRule extends InlineVariablesRule {

    public AsterixInlineVariablesRule() {
        // Do not inline field accesses and spatial functions because doing so would interfere with our access method rewrites.
        // TODO: For now we must also exclude record constructor functions to avoid breaking our type casting rules
        // IntroduceStaticTypeCastRule and IntroduceDynamicTypeCastRule.
        doNotInlineFuncs.add(BuiltinFunctions.FIELD_ACCESS_BY_NAME);
        doNotInlineFuncs.add(BuiltinFunctions.FIELD_ACCESS_BY_INDEX);
        doNotInlineFuncs.add(BuiltinFunctions.CLOSED_RECORD_CONSTRUCTOR);
        doNotInlineFuncs.add(BuiltinFunctions.OPEN_RECORD_CONSTRUCTOR);
        doNotInlineFuncs.add(BuiltinFunctions.CAST_TYPE);
        doNotInlineFuncs.add(BuiltinFunctions.CREATE_CIRCLE);
        doNotInlineFuncs.add(BuiltinFunctions.CREATE_LINE);
        doNotInlineFuncs.add(BuiltinFunctions.CREATE_MBR);
        doNotInlineFuncs.add(BuiltinFunctions.CREATE_POINT);
        doNotInlineFuncs.add(BuiltinFunctions.CREATE_POLYGON);
        doNotInlineFuncs.add(BuiltinFunctions.CREATE_RECTANGLE);
    }
}
