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
import org.apache.asterix.external.library.java.base.JInt;

public class MySumFunction implements IExternalScalarFunction {

    private JInt result;

    @Override
    public void deinitialize() {
        // nothing to do here
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        int arg0 = ((JInt) functionHelper.getArgument(0)).getValue();
        int arg1 = ((JInt) functionHelper.getArgument(1)).getValue();
        result.setValue(arg0 + arg1);
        functionHelper.setResult(result);
    }

    @Override
    public void initialize(IFunctionHelper functionHelper) {
        result = (JInt) functionHelper.getResultObject();
    }

}
