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

import java.util.Random;

import org.apache.asterix.external.library.java.JObjects.JInt;
import org.apache.asterix.external.library.java.JObjects.JRecord;
import org.apache.asterix.external.library.java.JObjects.JString;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JTypeTag;

/**
 * Accepts an input record of type Open{ id: int32, text: string }
 * Converts the text field into upper case and appends an additional field -
 * "substring" with value as a random substring of the text field.
 * Return Type Open{ id: int32, text: string }
 */
public class UpperCaseFunction implements IExternalScalarFunction {

    private Random random;

    @Override
    public void initialize(IFunctionHelper functionHelper) {
        random = new Random();
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JInt id = (JInt) inputRecord.getValueByName("id");
        id.setValue(id.getValue() * -1); // for maintaining uniqueness
                                         // constraint in the case when
                                         // output is re-inserted into source
                                         // dataset
        JString text = (JString) inputRecord.getValueByName("text");
        text.setValue(text.getValue().toUpperCase());
        JRecord result = (JRecord) functionHelper.getResultObject();
        result.setField("id", id);
        result.setField("text", text);
        functionHelper.setResult(result);
    }
}
