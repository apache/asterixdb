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

import java.io.InputStream;
import java.util.Properties;

import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JTypeTag;

public class CapitalFinderFunction implements IExternalScalarFunction {

    private static Properties capitalList;
    private static final String NOT_FOUND = "NOT_FOUND";
    private JString capital;

    @Override
    public void deinitialize() {
        System.out.println("De-Initialized");
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JString country = ((JString) functionHelper.getArgument(0));
        JRecord record = (JRecord) functionHelper.getResultObject();
        String capitalCity = capitalList.getProperty(country.getValue(), NOT_FOUND);
        capital.setValue(capitalCity);

        record.setField("country", country);
        record.setField("capital", capital);
        functionHelper.setResult(record);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        InputStream in = CapitalFinderFunction.class.getClassLoader().getResourceAsStream("data/countriesCapitals.txt");
        capitalList = new Properties();
        capitalList.load(in);
        capital = (JString) functionHelper.getObject(JTypeTag.STRING);
    }

}
