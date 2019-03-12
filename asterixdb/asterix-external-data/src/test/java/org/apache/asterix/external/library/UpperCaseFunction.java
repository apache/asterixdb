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
package org.apache.asterix.external.library;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JOrderedList;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.asterix.om.types.BuiltinType;

/**
 * Accepts an input record of type Open{ id: int32, text: string }
 * Converts the text field into upper case and appends an additional field -
 * "substring" with value as a random substring of the text field.
 * Return Type Open{ id: int32, text: string }
 */
public class UpperCaseFunction implements IExternalScalarFunction {

    @Override
    public void initialize(IFunctionHelper functionHelper) {
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JOrderedList textList = (JOrderedList) inputRecord.getValueByName("text_list");
        JOrderedList capList = new JOrderedList(BuiltinType.ASTRING);
        JInt id = (JInt) inputRecord.getValueByName("id");
        id.setValue(id.getValue() * -1);

        for (int iter1 = 0; iter1 < textList.getValue().size(); iter1++) {
            JRecord originalElement = (JRecord) textList.getValue().get(iter1);
            JString originalText = (JString) originalElement.getValueByName("text");
            JString capText = new JString(originalText.getValue().toUpperCase());
            capList.getValue().add(capText);
        }
        JInt element_n = new JInt(textList.size());
        JRecord result = (JRecord) functionHelper.getResultObject();
        result.setField("id", id);
        result.setField("text_list", textList);
        result.setField("element_n", element_n);
        result.setField("capitalized_list", capList);
        functionHelper.setResult(result);
    }
}
