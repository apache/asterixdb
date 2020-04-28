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

import java.io.InputStream;
import java.util.Properties;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.base.JRecord;
import org.apache.asterix.external.library.java.base.JString;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;

public class OpenCapitalFinderFunction implements IExternalScalarFunction {

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
        ARecordType recordType = new ARecordType("all", new String[] {}, new IAType[] {}, true);
        JRecord record = (JRecord) functionHelper.getResultObject(recordType);
        String capitalCity = capitalList.getProperty(country.getValueGeneric(), NOT_FOUND);
        capital.setValue(capitalCity);

        record.setField("country", country);
        record.setField("capital", capital);
        functionHelper.setResult(record);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        InputStream in = OpenCapitalFinderFunction.class.getClassLoader()
                .getResourceAsStream("data/countriesCapitals.properties");
        capitalList = new Properties();
        capitalList.load(in);
        capital = new JString("");
    }

}
