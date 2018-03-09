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

import java.util.Random;

import org.apache.asterix.external.api.IExternalScalarFunction;
import org.apache.asterix.external.api.IFunctionHelper;
import org.apache.asterix.external.library.java.base.JRecord;

public class EchoDelayFunction implements IExternalScalarFunction {

    private Random rand = new Random();
    private long sleepIntervalMin;
    private long sleepIntervalMax;
    private int range;

    @Override
    public void initialize(IFunctionHelper functionHelper) {
        sleepIntervalMin = 50;
        sleepIntervalMax = 100;
        range = (new Long(sleepIntervalMax - sleepIntervalMin)).intValue();
    }

    @Override
    public void deinitialize() {
    }

    @Override
    public void evaluate(IFunctionHelper functionHelper) throws Exception {
        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
        long sleepInterval = rand.nextInt(range);
        Thread.sleep(sleepInterval);
        functionHelper.setResult(inputRecord);
    }
}
