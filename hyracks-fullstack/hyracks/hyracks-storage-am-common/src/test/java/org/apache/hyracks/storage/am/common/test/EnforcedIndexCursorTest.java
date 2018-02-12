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

import java.util.ArrayList;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.EnforcedIndexCursor;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class EnforcedIndexCursorTest extends IIndexCursorTest {
    @Override
    protected List<ISearchPredicate> createSearchPredicates() {
        List<ISearchPredicate> predicates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            predicates.add(Mockito.mock(ISearchPredicate.class));
        }
        return predicates;
    }

    @Override
    protected IIndexAccessor createAccessor() throws HyracksDataException {
        EnforcedIndexCursor cursor = new DummyEnforcedIndexCursor();
        IIndexAccessor accessor = Mockito.mock(IIndexAccessor.class);
        Mockito.when(accessor.createSearchCursor(Mockito.anyBoolean())).thenReturn(cursor);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                IIndexCursor cursor = (IIndexCursor) args[0];
                ISearchPredicate pred = (ISearchPredicate) args[1];
                cursor.open(null, pred);
                return null;
            }
        }).when(accessor).search(Matchers.any(IIndexCursor.class), Matchers.any(ISearchPredicate.class));
        return accessor;
    }
}
