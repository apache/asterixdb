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

package org.apache.hyracks.storage.am.common.test;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.junit.Assert;
import org.junit.Test;

/**
 * This is a test class that forms the basis for unit tests of different implementations of the IIndexCursor interface
 */
public abstract class IIndexCursorTest {
    @Test
    public void testNormalLifeCycle() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        for (ISearchPredicate predicate : predicates) {
            open(accessor, cursor, predicate);
            while (cursor.hasNext()) {
                cursor.next();
            }
            cursor.close();
        }
        cursor.destroy();
        destroy(accessor);
    }

    protected void destroy(IIndexAccessor accessor) throws HyracksDataException {
        accessor.destroy();
    }

    @Test
    public void testCreateDestroySucceed() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        cursor.destroy();
        destroy(accessor);
    }

    @Test
    public void testDoubleOpenFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        boolean expectedExceptionThrown = false;
        try {
            open(accessor, cursor, predicates.get(0));
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.close();
        cursor.destroy();
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testCloseWithoutOpenSucceeds() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        cursor.close();
        cursor.destroy();
        destroy(accessor);
    }

    @Test
    public void testDoubleCloseSucceeds() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        cursor.close();
        cursor.destroy();
        destroy(accessor);
    }

    @Test
    public void testDoubleDestroySucceeds() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        cursor.destroy();
        cursor.destroy();
        destroy(accessor);
    }

    @Test
    public void testHasNextBeforeOpenFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        boolean expectedExceptionThrown = false;
        try {
            cursor.hasNext();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testHasNextAfterCloseFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        boolean expectedExceptionThrown = false;
        try {
            cursor.hasNext();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testNextBeforeOpenFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        boolean expectedExceptionThrown = false;
        try {
            cursor.next();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testNextAfterCloseFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        boolean expectedExceptionThrown = false;
        try {
            cursor.next();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testDestroyWhileOpenFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        boolean expectedExceptionThrown = false;
        try {
            cursor.destroy();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.close();
        cursor.destroy();
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testOpenAfterDestroyFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        cursor.destroy();
        boolean expectedExceptionThrown = false;
        try {
            open(accessor, cursor, predicates.get(0));
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testCloseAfterDestroyFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        cursor.destroy();
        boolean expectedExceptionThrown = false;
        try {
            cursor.close();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testNextAfterDestroyFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        cursor.destroy();
        boolean expectedExceptionThrown = false;
        try {
            cursor.next();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testHasNextAfterDestroyFails() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        cursor.destroy();
        boolean expectedExceptionThrown = false;
        try {
            cursor.hasNext();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        destroy(accessor);
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testGetTupleReturnsNullAfterDestroy() throws Exception {
        IIndexAccessor accessor = createAccessor();
        IIndexCursor cursor = createCursor(accessor);
        List<ISearchPredicate> predicates = createSearchPredicates();
        open(accessor, cursor, predicates.get(0));
        cursor.close();
        cursor.destroy();
        destroy(accessor);
        Assert.assertNull(cursor.getTuple());
    }

    protected IIndexCursor createCursor(IIndexAccessor accessor) throws Exception {
        return accessor.createSearchCursor(false);
    }

    protected void open(IIndexAccessor accessor, IIndexCursor cursor, ISearchPredicate predicate)
            throws HyracksDataException {
        accessor.search(cursor, predicate);
    }

    protected abstract List<ISearchPredicate> createSearchPredicates() throws Exception;

    protected abstract IIndexAccessor createAccessor() throws Exception;
}
