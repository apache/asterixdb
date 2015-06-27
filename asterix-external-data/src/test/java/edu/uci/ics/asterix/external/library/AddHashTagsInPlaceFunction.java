/*  1
 * Copyright 2009-2013 by The Regents of the University of California   2
             * Licensed under the Apache License, Version 2.0 (the "License");  3
 * you may not use this file except in compliance with the License. 4
 * you may obtain a copy of the License from    5
 *  6
 *     http://www.apache.org/licenses/LICENSE-2.0   
 *  
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,    
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and  
 * limitations under the License.   
 */
package edu.uci.ics.asterix.external.library;

import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.util.Datatypes;

public class AddHashTagsInPlaceFunction implements IExternalScalarFunction {

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
        JString text = (JString) inputRecord.getValueByName(Datatypes.Tweet.MESSAGE);

        String[] tokens = text.getValue().split(" ");
        for (String tk : tokens) {
            if (tk.startsWith("#")) {
                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                newField.setValue(tk);
                list.add(newField);
            }
        }
        inputRecord.addField(Datatypes.ProcessedTweet.TOPICS, list);
        functionHelper.setResult(inputRecord);
    }

}