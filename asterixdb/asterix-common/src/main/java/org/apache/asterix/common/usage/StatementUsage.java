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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import org.apache.asterix.common.usage.QueryOptimizer.RboReason;

/**
 * What one statement used, as recorded while it was compiled and run: the {@link QueryFeature}s it used, which
 * {@link QueryOptimizer} planned it, and how many documents it read or wrote where that is known. One instance belongs
 * to one statement, and is written from that statement's compilation thread only.
 */
public final class StatementUsage {

    /** {@link #getDocuments()} when the statement's document count is not known */
    public static final long UNKNOWN = -1;

    private final Set<QueryFeature> features = EnumSet.noneOf(QueryFeature.class);
    private boolean planned;
    private RboReason rboReason;
    private long documents = UNKNOWN;

    public void mark(QueryFeature feature) {
        features.add(feature);
    }

    /** @return the features marked so far, in declaration order */
    public Set<QueryFeature> getFeatures() {
        return Collections.unmodifiableSet(features);
    }

    /**
     * Records that the cost-based optimizer planned a part of the statement's plan. A statement can be planned in
     * parts, one for each join graph in it, so this does not override an earlier {@link #recordRbo(RboReason)}.
     */
    public void recordCbo() {
        planned = true;
    }

    /**
     * Records that a part of the statement's plan was left to the rule-based optimizer. A statement any part of which
     * fell back is reported as planned by RBO, with the most significant of its reasons.
     */
    public void recordRbo(RboReason reason) {
        planned = true;
        if (rboReason == null || reason.compareTo(rboReason) < 0) {
            rboReason = reason;
        }
    }

    /**
     * @return the optimizer that planned the statement, or null when no plan of it read a collection, so that no join
     *         order or access path was chosen
     */
    public QueryOptimizer getOptimizer() {
        if (!planned) {
            return null;
        }
        return rboReason != null ? QueryOptimizer.RBO : QueryOptimizer.CBO;
    }

    /** @return why the plan was left to RBO, or null when it was not */
    public RboReason getRboReason() {
        return rboReason;
    }

    public void setDocuments(long documents) {
        this.documents = documents;
    }

    /** @return the documents the statement read or wrote, or {@link #UNKNOWN} */
    public long getDocuments() {
        return documents;
    }

    /** Copies what compiling a plan recorded, for a statement that reuses that plan rather than compiling its own. */
    public void addCompiled(StatementUsage compiled) {
        features.addAll(compiled.features);
        planned |= compiled.planned;
        if (compiled.rboReason != null) {
            recordRbo(compiled.rboReason);
        }
    }

    /** @return a copy of what compilation recorded, without the document count, which belongs to one run */
    public StatementUsage compiledCopy() {
        StatementUsage copy = new StatementUsage();
        copy.addCompiled(this);
        return copy;
    }

    @Override
    public String toString() {
        return "{features: " + features + ", optimizer: " + getOptimizer() + ", rboReason: " + rboReason
                + ", documents: " + documents + "}";
    }
}
