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

import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
import edu.uci.ics.asterix.external.library.IFunctionHelper;
import edu.uci.ics.asterix.external.library.java.JObjects.JInt;

public class SumFunction implements IExternalScalarFunction {

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
