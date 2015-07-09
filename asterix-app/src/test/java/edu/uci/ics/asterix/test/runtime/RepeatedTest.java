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
package edu.uci.ics.asterix.test.runtime;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import edu.uci.ics.asterix.test.aql.TestsUtils;
import edu.uci.ics.asterix.test.runtime.RepeatRule.Repeat;
import edu.uci.ics.asterix.testframework.context.TestCaseContext;

/**
 * Runs runtime test cases that have been identified in the repeatedtestsuite.xml.
 * 
 * Each test is run 10000 times.
 */
class RepeatRule implements MethodRule {

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ java.lang.annotation.ElementType.METHOD })
    public @interface Repeat {
        public abstract int times();

    }

    private static class RepeatStatement extends Statement {

        private final int times;
        private final Statement statement;

        private RepeatStatement(int times, Statement statement) {
            this.times = times;
            this.statement = statement;
        }

        @Override
        public void evaluate() throws Throwable {
            for (int i = 0; i < times; i++) {
                statement.evaluate();
            }
        }
    }

    @Override
    public Statement apply(Statement statement, FrameworkMethod method, Object target) {
        Statement result = statement;
        Repeat repeat = method.getAnnotation(Repeat.class);
        if (repeat != null) {
            int times = repeat.times();
            result = new RepeatStatement(times, statement);
        }
        return result;

    }
}

@RunWith(Parameterized.class)
public class RepeatedTest extends ExecutionTest {

    private int count;
    
    @Parameters
    public static Collection<Object[]> tests() throws Exception {
        Collection<Object[]> testArgs = buildTestsInXml(TestCaseContext.DEFAULT_REPEADED_TESTSUITE_XML_NAME);
        return testArgs;
    }

    public RepeatedTest(TestCaseContext tcCtx) {
        super(tcCtx);
        count = 0;
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Test
    @Repeat(times = 10000)
    public void test() throws Exception {
        System.err.println("***** Test Count: " + (++count) + " ******");
        TestsUtils.executeTest(PATH_ACTUAL, tcCtx, null, false);
    }
}
