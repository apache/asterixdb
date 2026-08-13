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
package org.apache.asterix.translator;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.ResultSetId;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An interface that takes care of executing a list of statements that are submitted through an Asterix API
 */
public interface IStatementExecutor {

    /**
     * Specifies result delivery of executed statements
     */
    enum ResultDelivery {
        /**
         * Results are returned with the first response
         */
        IMMEDIATE("immediate"),
        /**
         * Results are produced completely, but only a result handle is returned
         */
        DEFERRED("deferred"),
        /**
         * A result handle is returned before the resutlts are complete
         */
        ASYNC("async");

        private static final Map<String, ResultDelivery> deliveryNames =
                Collections.unmodifiableMap(Arrays.stream(ResultDelivery.values())
                        .collect(Collectors.toMap(ResultDelivery::getName, Function.identity())));
        private final String name;

        ResultDelivery(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static ResultDelivery fromName(String name) {
            return deliveryNames.get(name);
        }
    }

    class ResultMetadata implements Serializable {
        private static final long serialVersionUID = 3L;

        private final List<ResultSetInfo> resultSets = new ArrayList<>();

        private final List<StatementInfo> statements = new ArrayList<>();

        public List<ResultSetInfo> getResultSets() {
            return resultSets;
        }

        /** One entry per statement that ran, in execution order, including statements with no result. */
        public List<StatementInfo> getStatements() {
            return statements;
        }
    }

    /** What one statement produced. Filled in on the CC, carried to the NC on the request's result metadata. */
    class StatementInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        /** Position among the statements the client sent, counting from 1. */
        private final int position;
        private final Statement.Kind kind;
        /** Names an extension statement, whose kind does not; null otherwise. */
        private final String name;
        private ResultSetInfo resultSet;
        private ExecutionPlans plans;
        private Stats stats;
        private Throwable error;
        private final List<Warning> warnings = new ArrayList<>();

        public StatementInfo(int position, Statement.Kind kind, String name) {
            this.position = position;
            this.kind = kind;
            this.name = name;
        }

        public int getPosition() {
            return position;
        }

        public Statement.Kind getKind() {
            return kind;
        }

        public String getName() {
            return name;
        }

        public ResultSetInfo getResultSet() {
            return resultSet;
        }

        public void setResultSet(ResultSetInfo resultSet) {
            this.resultSet = resultSet;
        }

        public ExecutionPlans getPlans() {
            return plans;
        }

        public void setPlans(ExecutionPlans plans) {
            this.plans = plans;
        }

        public Stats getStats() {
            return stats;
        }

        public void setStats(Stats stats) {
            this.stats = stats;
        }

        public Throwable getError() {
            return error;
        }

        public void setError(Throwable error) {
            this.error = error;
        }

        /** The warnings this statement raised, up to its max-warnings budget: the parser's first. */
        public List<Warning> getWarnings() {
            return warnings;
        }

        public void addWarnings(Collection<Warning> statementWarnings) {
            warnings.addAll(statementWarnings);
        }

        @Override
        public String toString() {
            return "StatementInfo{position=" + position + ", kind=" + kind + ", name=" + name + ", resultSet="
                    + resultSet + ", error="
                    + (error != null ? error.getClass().getName() + ": " + error.getMessage() : "null") + '}';
        }
    }

    class ResultSetInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private final JobId jobId;
        private final ResultSetId resultSetId;
        private final ARecordType recordType;

        public ResultSetInfo(JobId jobId, ResultSetId resultSetId, ARecordType recordType) {
            this.jobId = jobId;
            this.resultSetId = resultSetId;
            this.recordType = recordType;
        }

        public JobId getJobId() {
            return jobId;
        }

        public ResultSetId getResultSetId() {
            return resultSetId;
        }

        public ARecordType getRecordType() {
            return recordType;
        }
    }

    class Stats implements Serializable {
        private static final long serialVersionUID = 6834062614264666810L;

        public enum ProfileType {
            COUNTS("counts"),
            FULL("timings"),
            NONE("off");

            private static final Map<String, ProfileType> profileNames = Collections.unmodifiableMap(Arrays
                    .stream(ProfileType.values()).collect(Collectors.toMap(ProfileType::getName, Function.identity())));
            private final String name;

            ProfileType(String name) {
                this.name = name;
            }

            public String getName() {
                return name;
            }

            public static ProfileType fromName(String name) {
                return profileNames.get(name);
            }
        }

        private long count;
        private long size;
        private long processedObjects;
        private long queueWaitTime;
        private Profile profile;
        private ProfileType profileType;
        private long totalWarningsCount;
        private long compileTime;
        private double bufferCacheHitRatio;
        private long bufferCachePageReadCount;
        private long cloudReadRequestsCount;
        private long cloudPagesReadCount;
        private long cloudPagesPersistedCount;
        private boolean cachedPlan;

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public long getQueueWaitTimeNanos() {
            return queueWaitTime;
        }

        public long getProcessedObjects() {
            return processedObjects;
        }

        public void setProcessedObjects(long processedObjects) {
            this.processedObjects = processedObjects;
        }

        public long getTotalWarningsCount() {
            return totalWarningsCount;
        }

        /** Replaces the count, for a caller that knows the request's warnings better than its running total. */
        public void setTotalWarningsCount(long totalWarningsCount) {
            this.totalWarningsCount = totalWarningsCount;
        }

        public void updateTotalWarningsCount(long delta) {
            if (delta <= Long.MAX_VALUE - totalWarningsCount) {
                totalWarningsCount += delta;
            }
        }

        public void setQueueWaitTimeNanos(long queueWaitTimeNanos) {
            this.queueWaitTime = queueWaitTimeNanos;
        }

        public void setJobProfile(ObjectNode profile) {
            this.profile = new Profile(profile);
        }

        public ObjectNode getJobProfile() {
            return profile != null ? profile.getProfile() : null;
        }

        public ProfileType getProfileType() {
            return profileType;
        }

        public void setProfileType(ProfileType profileType) {
            this.profileType = profileType;
        }

        public void setCompileTimeNanos(long compileTimeNanos) {
            this.compileTime = compileTimeNanos;
        }

        public long getCompileTimeNanos() {
            return compileTime;
        }

        public void setBufferCacheHitRatio(double bufferCacheHitRatio) {
            this.bufferCacheHitRatio = bufferCacheHitRatio;
        }

        public double getBufferCacheHitRatio() {
            return bufferCacheHitRatio;
        }

        public void setBufferCachePageReadCount(long bufferCachePageReadCount) {
            this.bufferCachePageReadCount = bufferCachePageReadCount;
        }

        public long getBufferCachePageReadCount() {
            return bufferCachePageReadCount;
        }

        public void setCloudReadRequestsCount(long cloudReadRequestsCount) {
            this.cloudReadRequestsCount = cloudReadRequestsCount;
        }

        public long getCloudReadRequestsCount() {
            return cloudReadRequestsCount;
        }

        public void setCloudPagesReadCount(long cloudPagesReadCount) {
            this.cloudPagesReadCount = cloudPagesReadCount;
        }

        public long getCloudPagesReadCount() {
            return cloudPagesReadCount;
        }

        public void setCloudPagesPersistedCount(long cloudPagesPersistedCount) {
            this.cloudPagesPersistedCount = cloudPagesPersistedCount;
        }

        /** A copy, to hold the figures reached before a statement ran while this instance goes on accumulating. */
        public Stats snapshot() {
            Stats copy = new Stats();
            copy.count = count;
            copy.size = size;
            copy.processedObjects = processedObjects;
            copy.queueWaitTime = queueWaitTime;
            copy.profile = profile;
            copy.profileType = profileType;
            copy.totalWarningsCount = totalWarningsCount;
            copy.compileTime = compileTime;
            copy.bufferCacheHitRatio = bufferCacheHitRatio;
            copy.bufferCachePageReadCount = bufferCachePageReadCount;
            copy.cloudReadRequestsCount = cloudReadRequestsCount;
            copy.cloudPagesReadCount = cloudPagesReadCount;
            copy.cloudPagesPersistedCount = cloudPagesPersistedCount;
            return copy;
        }

        /**
         * What the statement that ran since {@code before} contributed. Only the row count and size accumulate over a
         * request; every other figure describes one job, so it is already this statement's - or, where it has not
         * changed, belongs to an earlier statement and this one produced none of its own.
         */
        public Stats since(Stats before) {
            Stats delta = snapshot();
            delta.count -= before.count;
            delta.size -= before.size;
            // set where the warnings are attributed: the request's total counts earlier statements more than once
            delta.totalWarningsCount = 0;
            delta.compileTime = ownOrNone(compileTime, before.compileTime);
            delta.queueWaitTime = ownOrNone(queueWaitTime, before.queueWaitTime);
            delta.processedObjects = ownOrNone(processedObjects, before.processedObjects);
            delta.cloudReadRequestsCount = ownOrNone(cloudReadRequestsCount, before.cloudReadRequestsCount);
            delta.cloudPagesReadCount = ownOrNone(cloudPagesReadCount, before.cloudPagesReadCount);
            delta.cloudPagesPersistedCount = ownOrNone(cloudPagesPersistedCount, before.cloudPagesPersistedCount);
            if (bufferCachePageReadCount == before.bufferCachePageReadCount) {
                delta.bufferCachePageReadCount = 0;
                delta.bufferCacheHitRatio = Double.NaN;
            }
            if (profile == before.profile) {
                delta.profile = null; // unchanged means the previous statement's profile, not this one's
            }
            return delta;
        }

        private static long ownOrNone(long now, long before) {
            return now == before ? 0 : now;
        }

        public long getCloudPagesPersistedCount() {
            return cloudPagesPersistedCount;
        }

        public boolean isCachedPlan() {
            return cachedPlan;
        }

        public void setCachedPlan(boolean cachedPlan) {
            this.cachedPlan = cachedPlan;
        }
    }

    class Profile implements Serializable {
        private static final long serialVersionUID = 4813321148252768376L;

        private transient ObjectNode profile;

        public Profile(ObjectNode profile) {
            this.profile = profile;
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            ObjectMapper om = new ObjectMapper();
            byte[] bytes = om.writeValueAsBytes(profile);
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            ObjectMapper om = new ObjectMapper();
            int length = in.readInt();
            profile = (ObjectNode) om.readTree(in.readNBytes(length));
        }

        public ObjectNode getProfile() {
            return profile;
        }
    }

    class StatementProperties implements Serializable {
        private static final long serialVersionUID = -1L;

        private Statement.Kind kind;
        private String name;

        public Statement.Kind getKind() {
            return kind;
        }

        public void setKind(Statement.Kind kind) {
            this.kind = kind;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public boolean isValid() {
            return kind != null && (kind != Statement.Kind.EXTENSION || name != null);
        }

        @Override
        public String toString() {
            return Statement.Kind.EXTENSION == kind ? String.valueOf(name) : String.valueOf(kind);
        }
    }

    /**
     * Compiles and executes a list of statements
     *
     * @param hcc
     * @param requestParameters
     * @throws Exception
     */
    void compileAndExecute(IHyracksClientConnection hcc, IRequestParameters requestParameters) throws Exception;

    /**
     * rewrites and compiles query into a hyracks job specifications
     *
     * @param clusterInfoCollector
     *            The cluster info collector
     * @param metadataProvider
     *            The metadataProvider used to access metadata and build runtimes
     * @param query
     *            The query to be compiled
     * @param dmlStatement
     *            The data modification statement when the query results in a modification to a dataset
     * @param statementParameters
     *            Statement parameters
     * @param requestParameters
     *            The request parameters
     * @return the compiled {@code JobSpecification}
     * @throws AsterixException
     * @throws RemoteException
     * @throws AlgebricksException
     * @throws ACIDException
     */
    JobSpecification rewriteCompileQuery(IClusterInfoCollector clusterInfoCollector, MetadataProvider metadataProvider,
            Query query, ICompiledDmlStatement dmlStatement, Map<String, IAObject> statementParameters,
            IRequestParameters requestParameters) throws RemoteException, AlgebricksException, ACIDException;

    Namespace getActiveNamespace(Namespace namespace);

    /**
     * Gets the execution plans that are generated during query compilation
     *
     * @return the executions plans
     */
    ExecutionPlans getExecutionPlans();

    /**
     * Gets the response printer
     *
     * @return the responer printer
     */
    IResponsePrinter getResponsePrinter();

    /**
     * Gets the warnings generated during compiling and executing a request up to the max number argument.
     */
    void getWarnings(Collection<? super Warning> outWarnings, long maxWarnings);
}
