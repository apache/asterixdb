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
package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.external.library.java.JObjects.JDouble;
import edu.uci.ics.asterix.external.library.java.JObjects.JPoint;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.util.Datatypes;

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
