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
package org.apache.asterix.test.server;

import java.lang.reflect.Executable;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.test.support.FormattedExceptionTestBase;
import org.junit.runners.Parameterized;

public class FormattedExceptionInstanceTest extends FormattedExceptionTestBase {
    private static final ErrorCode ASK_ERROR_CODE = random(ErrorCode.values());
    private static final FunctionIdentifier FUNCTION_IDENTIFIER = new FunctionIdentifier("fake", "fake", "fake", 0);
    static {
        classSelector = classname -> classname.matches("^org\\.apache\\.(asterix|hyracks)\\..*");
    }

    public FormattedExceptionInstanceTest(String desc, Executable action, Class<? extends IFormattedException> root) {
        super(desc, action, root);
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() throws ClassNotFoundException {
        return defineParameters();
    }

    @Override
    protected Object defaultValue(Class type) {
        switch (type.getName()) {
            case "org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier":
                return FUNCTION_IDENTIFIER;
            case "org.apache.asterix.common.exceptions.ErrorCode":
                return ASK_ERROR_CODE;
            default:
                return super.defaultValue(type);
        }
    }
}
