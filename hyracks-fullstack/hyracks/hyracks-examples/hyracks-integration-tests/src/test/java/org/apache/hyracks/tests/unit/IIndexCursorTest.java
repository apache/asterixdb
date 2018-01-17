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

package org.apache.hyracks.tests.unit;

import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * This is a test class that forms the basis for unit tests of different implementations of the IIndexCursor interface
 */
public abstract class IIndexCursorTest {
    @Test
    public void testNormalLifeCycle() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        for (ISearchPredicate predicate : predicates) {
            cursor.open(initialState, predicate);
            while (cursor.hasNext()) {
                cursor.next();
            }
            cursor.close();
        }
        cursor.destroy();
    }

    @Test
    public void testCreateDestroySucceed() throws Exception {
        IIndexCursor cursor = createCursor();
        cursor.destroy();
    }

    @Test
    public void testDoubleOpenFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        boolean expectedExceptionThrown = false;
        try {
            cursor.open(initialState, predicates.get(0));
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.close();
        cursor.destroy();
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testCloseWithoutOpenFails() throws Exception {
        IIndexCursor cursor = createCursor();
        boolean expectedExceptionThrown = false;
        try {
            cursor.close();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testDoubleCloseFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        cursor.close();
        boolean expectedExceptionThrown = false;
        try {
            cursor.close();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testHasNextBeforeOpenFails() throws Exception {
        IIndexCursor cursor = createCursor();
        boolean expectedExceptionThrown = false;
        try {
            cursor.hasNext();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testHasNextAfterCloseFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        cursor.close();
        boolean expectedExceptionThrown = false;
        try {
            cursor.hasNext();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testNextBeforeOpenFails() throws Exception {
        IIndexCursor cursor = createCursor();
        boolean expectedExceptionThrown = false;
        try {
            cursor.next();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testNextAfterCloseFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        cursor.close();
        boolean expectedExceptionThrown = false;
        try {
            cursor.next();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.destroy();
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testDestroyWhileOpenFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        boolean expectedExceptionThrown = false;
        try {
            cursor.destroy();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        cursor.close();
        cursor.destroy();
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testOpenAfterDestroyFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        cursor.close();
        cursor.destroy();
        boolean expectedExceptionThrown = false;
        try {
            cursor.open(initialState, predicates.get(0));
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testCloseAfterDestroyFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        cursor.close();
        cursor.destroy();
        boolean expectedExceptionThrown = false;
        try {
            cursor.close();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testNextAfterDestroyFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        cursor.close();
        cursor.destroy();
        boolean expectedExceptionThrown = false;
        try {
            cursor.next();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testHasNextAfterDestroyFails() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        cursor.close();
        cursor.destroy();
        boolean expectedExceptionThrown = false;
        try {
            cursor.hasNext();
        } catch (Exception e) {
            expectedExceptionThrown = true;
        }
        Assert.assertTrue(expectedExceptionThrown);
    }

    @Test
    public void testGetTupleReturnsNullAfterDestroy() throws Exception {
        IIndexCursor cursor = createCursor();
        ICursorInitialState initialState = createCursorInitialState();
        List<ISearchPredicate> predicates = createSearchPredicates();
        cursor.open(initialState, predicates.get(0));
        cursor.close();
        cursor.destroy();
        Assert.assertNull(cursor.getTuple());
    }

    protected abstract List<ISearchPredicate> createSearchPredicates();

    protected abstract ICursorInitialState createCursorInitialState();

    protected abstract IIndexCursor createCursor();
}
