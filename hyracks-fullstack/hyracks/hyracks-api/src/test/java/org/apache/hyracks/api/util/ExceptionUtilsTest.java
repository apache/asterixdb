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
package org.apache.hyracks.api.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.hyracks.api.exceptions.IError;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.Assert;
import org.junit.Test;

@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "cover matchingCause walking the whole cause chain and terminating on a cyclic one")
public class ExceptionUtilsTest {

    private static final class Marker extends Exception {
        private static final long serialVersionUID = 1L;

        Marker(String message) {
            super(message);
        }

        Marker(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** the minimum an unwrap() cycle needs: a formatted exception whose cause is still settable */
    private static final class Formatted extends Exception implements IFormattedException {
        private static final long serialVersionUID = 1L;

        Formatted(String message) {
            super(message);
        }

        @Override
        public String getComponent() {
            return "TEST";
        }

        @Override
        public int getErrorCode() {
            return 1;
        }

        @Override
        public Optional<IError> getError() {
            return Optional.empty();
        }

        @Override
        public SourceLocation getSourceLocation() {
            return null;
        }

        @Override
        public Serializable[] getParams() {
            return new Serializable[0];
        }
    }

    @Test
    public void matchesTheThrowableItself() {
        Throwable e = new Marker("target");
        Assert.assertTrue(ExceptionUtils.matchingCause(e, Marker.class::isInstance));
    }

    @Test
    public void matchesTheImmediateCause() {
        Throwable e = new Exception("outer", new Marker("target"));
        Assert.assertTrue(ExceptionUtils.matchingCause(e, Marker.class::isInstance));
    }

    @Test
    public void matchesACauseBeyondTheFirst() {
        // the whole point: a match three links down must still be found
        Throwable e = new Exception("l0", new Exception("l1", new Exception("l2", new Marker("target"))));
        Assert.assertTrue(ExceptionUtils.matchingCause(e, Marker.class::isInstance));
    }

    @Test
    public void reportsNoMatchWhenNothingInTheChainMatches() {
        Throwable e = new Exception("l0", new Exception("l1", new Exception("l2")));
        Assert.assertFalse(ExceptionUtils.matchingCause(e, Marker.class::isInstance));
    }

    @Test
    public void visitsEveryLinkOfTheChain() {
        Throwable deepest = new Exception("l3");
        Throwable e = new Exception("l0", new Exception("l1", new Exception("l2", deepest)));
        List<String> visited = new ArrayList<>();
        Assert.assertFalse(ExceptionUtils.matchingCause(e, candidate -> {
            visited.add(candidate.getMessage());
            return false;
        }));
        // causes nearest-first, then the throwable itself: ConnectLinkStatement relies on that ordering, since
        // its predicate records the match it finds
        Assert.assertEquals(List.of("l1", "l2", "l3", "l0"), visited);
    }

    @Test(timeout = 10000)
    public void terminatesOnACyclicChain() {
        Exception first = new Exception("first");
        Exception second = new Exception("second", first);
        first.initCause(second);
        Assert.assertFalse(ExceptionUtils.matchingCause(first, Marker.class::isInstance));
        Assert.assertTrue(ExceptionUtils.matchingCause(first, candidate -> "second".equals(candidate.getMessage())));
    }

    @Test
    public void rootCauseIsTheDeepestLink() {
        Throwable deepest = new Marker("deepest");
        Throwable e = new Exception("l0", new Exception("l1", deepest));
        Assert.assertSame(deepest, ExceptionUtils.getRootCause(e));
    }

    @Test(timeout = 10000)
    public void rootCauseTerminatesOnACyclicChain() {
        Exception first = new Exception("first");
        Exception second = new Exception("second", first);
        first.initCause(second);
        Assert.assertNotNull(ExceptionUtils.getRootCause(first));
    }

    @Test
    public void unwrapStopsAtTheLastFormattedException() {
        Throwable plain = new Exception("plain");
        Formatted inner = new Formatted("inner");
        inner.initCause(plain);
        Formatted outer = new Formatted("outer");
        outer.initCause(inner);
        Assert.assertSame(inner, ExceptionUtils.unwrap(outer));
    }

    @Test(timeout = 10000)
    public void unwrapTerminatesOnACyclicChain() {
        Formatted first = new Formatted("first");
        Formatted second = new Formatted("second");
        second.initCause(first);
        first.initCause(second);
        Assert.assertNotNull(ExceptionUtils.unwrap(first));
    }

    @Test(timeout = 10000)
    public void terminatesOnALongerCyclicChain() {
        Exception a = new Exception("a");
        Exception c = new Exception("c", a);
        Exception b = new Exception("b", c);
        a.initCause(b);
        Assert.assertFalse(ExceptionUtils.matchingCause(a, Marker.class::isInstance));
    }
}
