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
package org.apache.asterix.column.filter.iterable.evaluator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.junit.Test;

/**
 * A column values reader left in an inconsistent state can make the filter evaluators loop without bound. The
 * loops are pure CPU over already-pinned pages, so a cancelled job's interrupt never surfaces on its own and
 * the task thread is stranded for the life of the process (MB-72389). Each case below builds a reader that
 * never terminates its loop and asserts that an interrupt delivered *while the loop is running* aborts it.
 */
public class IterableFilterEvaluatorInterruptTest {

    /** {@code isRepeatedValue() && !isLastDelimiter()} never goes false, so the repeated-value loop spins. */
    @Test
    public void repeatedValueLoopAbortsOnInterrupt() throws Exception {
        IColumnValuesReader primaryKey = stubReader(Map.of("next", true, "isValue", true));
        IColumnValuesReader repeated = stubReader(Map.of("next", true, "isRepeated", true, "isRepeatedValue", true));
        assertAbortsOnInterrupt(
                new ColumnarRepeatedIterableFilterEvaluator(alwaysFalse(), List.of(primaryKey, repeated)));
    }

    /** Every primary key value reads as missing, so the skip loop in {@code next()} spins. */
    @Test
    public void missingPrimaryKeyLoopAbortsOnInterrupt() throws Exception {
        IColumnValuesReader primaryKey = stubReader(Map.of("next", true, "isMissing", true));
        assertAbortsOnInterrupt(new ColumnIterableFilterEvaluator(alwaysFalse(), List.of(primaryKey)));
    }

    private static void assertAbortsOnInterrupt(IColumnIterableFilterEvaluator evaluator) throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            started.countDown();
            try {
                evaluator.evaluate();
            } catch (Throwable th) {
                failure.set(th);
            }
        }, "filter-interrupt-test");
        // the thread is left running if the evaluator ignores the interrupt; don't hold up JVM exit on it
        worker.setDaemon(true);
        worker.start();
        assertTrue("worker never started", started.await(10, TimeUnit.SECONDS));
        // let the loop actually be spinning, so the interrupt lands mid-loop rather than before it
        Thread.sleep(200);
        worker.interrupt();
        worker.join(TimeUnit.SECONDS.toMillis(20));

        assertFalse("filter evaluation did not react to the interrupt", worker.isAlive());
        Throwable th = failure.get();
        assertNotNull("filter evaluation returned instead of failing", th);
        // skipInterruptedCheck: the interrupt was delivered to the worker, not to this thread
        assertTrue("not an interrupt failure: " + th, ExceptionUtils.causedByInterrupt(th, true));
    }

    private static IScalarEvaluator alwaysFalse() {
        byte[] falseValue = { ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG, 0 };
        return (tuple, result) -> result.set(falseValue, 0, falseValue.length);
    }

    /**
     * A reader answering {@code true} for the named predicates and a zero/null default for everything else.
     * A proxy rather than a hand-written stub: the interface has upwards of twenty methods and these tests
     * care about four of them.
     */
    private static IColumnValuesReader stubReader(Map<String, Boolean> trueFor) {
        return (IColumnValuesReader) Proxy.newProxyInstance(IColumnValuesReader.class.getClassLoader(),
                new Class<?>[] { IColumnValuesReader.class }, (proxy, method, args) -> {
                    Boolean answer = trueFor.get(method.getName());
                    if (answer != null) {
                        return answer;
                    }
                    Class<?> returnType = method.getReturnType();
                    if (returnType == boolean.class) {
                        return Boolean.FALSE;
                    } else if (returnType == int.class) {
                        return 0;
                    } else if (returnType == long.class) {
                        return 0L;
                    } else if (returnType == float.class) {
                        return 0f;
                    } else if (returnType == double.class) {
                        return 0d;
                    } else if (returnType == String.class) {
                        return "stubReader";
                    }
                    return null;
                });
    }
}
