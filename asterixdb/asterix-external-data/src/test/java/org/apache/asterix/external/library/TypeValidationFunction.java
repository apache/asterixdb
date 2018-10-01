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
import org.apache.asterix.external.library.java.base.JBoolean;
import org.apache.asterix.external.library.java.base.JCircle;
import org.apache.asterix.external.library.java.base.JDate;
import org.apache.asterix.external.library.java.base.JDateTime;
import org.apache.asterix.external.library.java.base.JDouble;
import org.apache.asterix.external.library.java.base.JFloat;
import org.apache.asterix.external.library.java.base.JInt;
import org.apache.asterix.external.library.java.base.JLine;
import org.apache.asterix.external.library.java.base.JPoint;
import org.apache.asterix.external.library.java.base.JRectangle;
import org.apache.asterix.external.library.java.base.JString;

public class TypeValidationFunction implements IExternalScalarFunction {

    private JString result;

    @Override
    public void deinitialize() {
        // no op
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JInt int32 = (JInt) functionHelper.getArgument(0);
        JFloat floatVal = (JFloat) functionHelper.getArgument(1);
        JString stringVal = (JString) functionHelper.getArgument(2);
        JDouble doubleVal = (JDouble) functionHelper.getArgument(3);
        JBoolean booleanVal = (JBoolean) functionHelper.getArgument(4);
        JPoint pointVal = (JPoint) functionHelper.getArgument(5);
        JDate dateVal = (JDate) functionHelper.getArgument(6);
        JDateTime datetimeVal = (JDateTime) functionHelper.getArgument(7);
        JLine lineVal = (JLine) functionHelper.getArgument(8);
        JCircle circleVal = (JCircle) functionHelper.getArgument(9);
        JRectangle rectangleVal = (JRectangle) functionHelper.getArgument(10);

        StringBuilder sb = new StringBuilder();
        sb.append(int32.getIAObject() + " ");
        sb.append(floatVal.getIAObject() + " ");
        sb.append(stringVal.getIAObject() + " ");
        sb.append(doubleVal.getIAObject() + " ");
        sb.append(booleanVal.getIAObject() + " ");
        sb.append(pointVal.getIAObject() + " ");
        sb.append(dateVal.getIAObject() + " ");
        sb.append(datetimeVal.getIAObject() + " ");
        sb.append(lineVal.getIAObject() + " ");
        sb.append(circleVal.getIAObject() + " ");
        sb.append(rectangleVal.getIAObject());
        result.setValue(sb.toString());
        functionHelper.setResult(result);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) throws Exception {
        result = new JString("");
    }

}
