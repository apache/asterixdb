/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.asterix.common.usage;

import java.util.EnumSet;

import org.apache.asterix.common.usage.QueryOptimizer.RboReason;
import org.junit.Assert;
import org.junit.Test;

public class StatementUsageTest {

    @Test
    public void aStatementNothingPlannedHasNoOptimizer() {
        StatementUsage usage = new StatementUsage();
        Assert.assertNull(usage.getOptimizer());
        Assert.assertNull(usage.getRboReason());
        Assert.assertTrue(usage.getFeatures().isEmpty());
        Assert.assertEquals(StatementUsage.UNKNOWN, usage.getDocuments());
    }

    @Test
    public void aStatementPlannedOnlyByCboIsCbo() {
        StatementUsage usage = new StatementUsage();
        usage.recordCbo();
        usage.recordCbo();
        Assert.assertEquals(QueryOptimizer.CBO, usage.getOptimizer());
        Assert.assertNull(usage.getRboReason());
    }

    @Test
    public void anyPartLeftToRboMakesTheStatementRbo() {
        StatementUsage usage = new StatementUsage();
        usage.recordCbo();
        usage.recordRbo(RboReason.OTHER);
        usage.recordCbo();
        Assert.assertEquals(QueryOptimizer.RBO, usage.getOptimizer());
        Assert.assertEquals(RboReason.OTHER, usage.getRboReason());
    }

    @Test
    public void theMostSignificantReasonWins() {
        StatementUsage usage = new StatementUsage();
        usage.recordRbo(RboReason.TOO_MANY_JOINS);
        usage.recordRbo(RboReason.MISSING_SAMPLES);
        usage.recordRbo(RboReason.OTHER);
        Assert.assertEquals(RboReason.MISSING_SAMPLES, usage.getRboReason());
    }

    @Test
    public void eachFeatureIsMarkedOnce() {
        StatementUsage usage = new StatementUsage();
        usage.mark(QueryFeature.VECTOR_SEARCH);
        usage.mark(QueryFeature.CLUSTER_BY);
        usage.mark(QueryFeature.VECTOR_SEARCH);
        Assert.assertEquals(EnumSet.of(QueryFeature.CLUSTER_BY, QueryFeature.VECTOR_SEARCH), usage.getFeatures());
    }

    @Test
    public void aCachedPlanRecordsWhatCompilingItRecordedButNotItsDocuments() {
        StatementUsage compiled = new StatementUsage();
        compiled.mark(QueryFeature.PYTHON_UDF);
        compiled.recordRbo(RboReason.MISSING_SAMPLES);
        compiled.setDocuments(42);
        StatementUsage cached = compiled.compiledCopy();
        Assert.assertEquals(StatementUsage.UNKNOWN, cached.getDocuments());

        StatementUsage reused = new StatementUsage();
        reused.mark(QueryFeature.ADVISE);
        reused.addCompiled(cached);
        Assert.assertEquals(EnumSet.of(QueryFeature.ADVISE, QueryFeature.PYTHON_UDF), reused.getFeatures());
        Assert.assertEquals(QueryOptimizer.RBO, reused.getOptimizer());
        Assert.assertEquals(RboReason.MISSING_SAMPLES, reused.getRboReason());
    }

    @Test
    public void labelsAreLowerCase() {
        Assert.assertEquals("cluster_by", QueryFeature.CLUSTER_BY.label());
        Assert.assertEquals("cbo", QueryOptimizer.CBO.label());
        Assert.assertEquals("missing_samples", RboReason.MISSING_SAMPLES.label());
    }
}
