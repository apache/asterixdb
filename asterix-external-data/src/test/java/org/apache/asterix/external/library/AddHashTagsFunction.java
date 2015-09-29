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

import org.apache.asterix.external.library.java.JObjects.JDouble;
import org.apache.asterix.external.library.java.JObjects.JPoint;
import org.apache.asterix.external.library.java.JObjects.JRecord;
import org.apache.asterix.external.library.java.JObjects.JString;
import org.apache.asterix.external.library.java.JObjects.JUnorderedList;
import org.apache.asterix.external.library.java.JTypeTag;
import org.apache.asterix.external.util.Datatypes;

public class AddHashTagsFunction implements IExternalScalarFunction {

    private JUnorderedList list = null;
    private JPoint location = null;

    @Override
    public void initialize(IFunctionHelper functionHelper) {
        list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
        location = new JPoint(0, 0);
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        list.clear();
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        JString text = (JString) inputRecord.getValueByName(Datatypes.Tweet.MESSAGE);
        JDouble latitude = (JDouble) inputRecord.getValueByName(Datatypes.Tweet.LATITUDE);
        JDouble longitude = (JDouble) inputRecord.getValueByName(Datatypes.Tweet.LONGITUDE);

        if (latitude != null && longitude != null) {
            location.setValue(latitude.getValue(), longitude.getValue());
        } else {
            location.setValue(0, 0);
        }

        String[] tokens = text.getValue().split(" ");
        for (String tk : tokens) {
            if (tk.startsWith("#")) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(tk);
                list.add(newField);
            }
        }

        JRecord outputRecord = (JRecord) functionHelper.getResultObject();
        outputRecord.setField(Datatypes.Tweet.ID, inputRecord.getValueByName(Datatypes.Tweet.ID));

        JRecord userRecord = (JRecord) inputRecord.getValueByName(Datatypes.Tweet.USER);
        outputRecord.setField(Datatypes.ProcessedTweet.USER_NAME,
                userRecord.getValueByName(Datatypes.Tweet.SCREEN_NAME));

        outputRecord.setField(Datatypes.ProcessedTweet.LOCATION, location);
        outputRecord.setField(Datatypes.Tweet.CREATED_AT, inputRecord.getValueByName(Datatypes.Tweet.CREATED_AT));
        outputRecord.setField(Datatypes.Tweet.MESSAGE, text);
        outputRecord.setField(Datatypes.ProcessedTweet.TOPICS, list);

        inputRecord.addField(Datatypes.ProcessedTweet.TOPICS, list);
        functionHelper.setResult(outputRecord);
    }

}
