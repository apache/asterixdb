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

import org.apache.asterix.external.library.java.JObjects.JRecord;
import org.apache.asterix.external.library.java.JObjects.JString;
import org.apache.asterix.external.library.java.JObjects.JUnorderedList;
import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.JTypeTag;

public class ParseTweetFunction implements IExternalScalarFunction {

    private JUnorderedList list = null;

    @Override
    public void initialize(IFunctionHelper functionHelper) {
        list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        list.clear();
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JString id = (JString) inputRecord.getValueByName("id");
        JString text = (JString) inputRecord.getValueByName("text");

        String[] tokens = text.getValue().split(" ");
        for (String tk : tokens) {
            if (tk.startsWith("#")) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(tk);
                list.add(newField);
            }
        }
        JRecord result = (JRecord) functionHelper.getResultObject();
        result.setField("id", id);
        result.setField("username", inputRecord.getValueByName("username"));
        result.setField("location", inputRecord.getValueByName("location"));
        result.setField("text", text);
        result.setField("timestamp", inputRecord.getValueByName("timestamp"));
        result.setField("topics", list);
        functionHelper.setResult(result);
    }

}
