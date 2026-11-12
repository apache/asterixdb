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
package org.apache.asterix.column.test.explosion;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.filter.FilterAccessorProvider;
import org.apache.asterix.column.filter.TrueColumnFilterEvaluator;
import org.apache.asterix.column.filter.iterable.ColumnFilterEvaluatorContext;
import org.apache.asterix.column.filter.iterable.IColumnIterableFilterEvaluator;
import org.apache.asterix.column.filter.iterable.accessor.ColumnFilterValueAccessorEvaluatorFactory;
import org.apache.asterix.column.filter.iterable.evaluator.ColumnIterableFilterEvaluatorFactory;
import org.apache.asterix.column.metadata.schema.visitor.SchemaClipperVisitor;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.NoOpWarningCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A pushed-down filter on a collapsed node: a conjunct whose path may hold its value in a map entry cannot filter,
 * as no filter reader sees the map; the filter's other conjuncts still can.
 */
public class CollapsedMapFilterTest {
    private static FlushColumnMetadata schema;

    @Before
    public void configure() throws Exception {
        CollapseSettings.install(CollapseSettings.lowered(16, 1));
        List<String> lines = new ArrayList<>();
        for (int d = 0; d < 200; d++) {
            lines.add("{\"id\": " + d + ", \"k" + d + "\": " + d + "}");
        }
        schema = WideValueCollapseTest.schemaOf(lines);
        assertTrue("the fixture must collapse the root", schema.getRoot().isCollapsed());
    }

    @After
    public void restore() {
        CollapseSettings.reset();
    }

    @Test
    public void aConjunctOnAColumnStillFiltersBesideOneOnTheMap() throws Exception {
        FilterAccessorProvider provider = provider();
        IColumnIterableFilterEvaluator filter =
                new ColumnIterableFilterEvaluatorFactory(accessor("id"), accessor("k500")).create(context(provider));
        assertNotSame("the conjunct on id must still filter", TrueColumnFilterEvaluator.INSTANCE, filter);
        assertFalse("id's reader must be kept", provider.getFilterColumnReaders().isEmpty());
    }

    @Test
    public void aFilterWhoseEveryConjunctMayBeInTheMapFiltersNothing() throws Exception {
        FilterAccessorProvider provider = provider();
        assertSame("k500 may sit in a map entry", TrueColumnFilterEvaluator.INSTANCE,
                new ColumnIterableFilterEvaluatorFactory(accessor("k500")).create(context(provider)));
    }

    private static FilterAccessorProvider provider() {
        SchemaClipperVisitor clipper = new SchemaClipperVisitor(schema.getFieldNamesDictionary(),
                Collections.emptyMap(), NoOpWarningCollector.INSTANCE);
        return new FilterAccessorProvider(schema.getRoot(), clipper, new ColumnValueReaderFactory(),
                ValueGetterFactory.INSTANCE);
    }

    private static ColumnFilterEvaluatorContext context(FilterAccessorProvider provider) {
        return new ColumnFilterEvaluatorContext(stub(IHyracksTaskContext.class), provider);
    }

    /** An interface whose getters return stubs of their own, and a no-op warning collector: all a filter asks of it. */
    private static <T> T stub(Class<T> type) {
        return type
                .cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] { type }, (proxy, method, args) -> {
                    Class<?> returned = method.getReturnType();
                    if (returned == IWarningCollector.class) {
                        return NoOpWarningCollector.INSTANCE;
                    }
                    return returned.isInterface() ? stub(returned) : null;
                }));
    }

    private static IScalarEvaluatorFactory accessor(String field) {
        return new ColumnFilterValueAccessorEvaluatorFactory(
                new ARecordType("root", new String[] { field }, new IAType[] { BuiltinType.ANY }, true));
    }
}
