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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.base.JBoolean;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;

public class KeywordsDetectorFunction implements IExternalScalarFunction {

    private ArrayList<String> keywordsList;
    private String dictPath, fieldName;
    private List<String> functionParameters;

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JRecord outputRecord = (JRecord) functionHelper.getResultObject();
        JBoolean chkVal = new JBoolean(false);
        String fieldValue = ((JString) inputRecord.getValueByName(fieldName)).getValue();

        chkVal.setValue(keywordsList.contains(fieldValue));

        outputRecord.setField("id", inputRecord.getValueByName("id"));
        outputRecord.setField("sensitive", chkVal);
        functionHelper.setResult(outputRecord);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        keywordsList = new ArrayList<>();
        functionParameters = functionHelper.getParameters();
        if (functionParameters.size() < 2) {
            throw new IllegalArgumentException("Expected more parameters. Please check your UDF configuration.");
        }
        dictPath = functionParameters.get(0);
        fieldName = functionParameters.get(1);
        Files.lines(Paths.get(dictPath)).forEach(keyword -> keywordsList.add(keyword));
    }

    @Override
    public void deinitialize() {
        // no op
    }
}
