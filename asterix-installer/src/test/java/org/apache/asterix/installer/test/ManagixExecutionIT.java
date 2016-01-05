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
package org.apache.asterix.installer.test;

import org.apache.asterix.testframework.context.TestCaseContext;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Runs the runtime test cases under 'asterix-app/src/test/resources/runtimets'.
 */
@RunWith(Parameterized.class)
public class ManagixExecutionIT extends AbstractExecutionIT {

    private TestCaseContext tcCtx;

    public ManagixExecutionIT(TestCaseContext tcCtx) {
        super(tcCtx);
        this.tcCtx = tcCtx;
    }
}
