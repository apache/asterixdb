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
import org.apache.asterix.external.library.java.base.JString;

public class WordInListFunction implements IExternalScalarFunction {

    private ArrayList<String> keywordsList;
    private String dictPath;
    private List<String> functionParameters;

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JString input = (JString) functionHelper.getArgument(0);
        JBoolean output = (JBoolean) functionHelper.getResultObject();
        String fieldValue = input.getValue();
        boolean contains = keywordsList.contains(fieldValue);
        output.setValue(contains);
        functionHelper.setResult(output);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        keywordsList = new ArrayList<>();
        functionParameters = functionHelper.getParameters();
        dictPath = functionParameters.get(0);
        Files.lines(Paths.get(dictPath)).forEach(keyword -> keywordsList.add(keyword));
    }

    @Override
    public void deinitialize() {
        // no op
    }
}
