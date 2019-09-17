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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IResponsePrinter;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.IStatementRewriter;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IClusterInfoCollector;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.result.ResultSetId;

import com.fasterxml.jackson.databind.JsonNode;
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
        IMMEDIATE,
        /**
         * Results are produced completely, but only a result handle is returned
         */
        DEFERRED,
        /**
         * A result handle is returned before the resutlts are complete
         */
        ASYNC
    }

    class ResultMetadata implements Serializable {
        private static final long serialVersionUID = 1L;

        private final List<Triple<JobId, ResultSetId, ARecordType>> resultSets = new ArrayList<>();

        public List<Triple<JobId, ResultSetId, ARecordType>> getResultSets() {
            return resultSets;
        }

    }

    class Stats implements Serializable {
        private static final long serialVersionUID = 5885273238208454611L;

        public enum ProfileType {
            COUNTS,
            FULL
        }

        private long count;
        private long size;
        private long processedObjects;
        private Profile profile;
        private ProfileType type;
        private long diskIoCount;
        private long totalWarningsCount;

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

        public long getProcessedObjects() {
            return processedObjects;
        }

        public void setProcessedObjects(long processedObjects) {
            this.processedObjects = processedObjects;
        }

        public long getDiskIoCount() {
            return diskIoCount;
        }

        public void setDiskIoCount(long diskIoCount) {
            this.diskIoCount = diskIoCount;
        }

        public long getTotalWarningsCount() {
            return totalWarningsCount;
        }

        public void updateTotalWarningsCount(long delta) {
            if (delta <= Long.MAX_VALUE - totalWarningsCount) {
                totalWarningsCount += delta;
            }
        }

        public void setJobProfile(ObjectNode profile) {
            this.profile = new Profile(profile);
        }

        public ObjectNode getJobProfile() {
            return profile != null ? profile.getProfile() : null;
        }

        public ProfileType getType() {
            return type;
        }

        public void setType(ProfileType type) {
            this.type = type;
        }
    }

    class Profile implements Serializable {
        private transient ObjectNode profile;

        public Profile(ObjectNode profile) {
            this.profile = profile;
        }

        private void writeObject(ObjectOutputStream out) throws IOException {
            ObjectMapper om = new ObjectMapper();
            out.writeUTF(om.writeValueAsString(profile));
        }

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            ObjectMapper om = new ObjectMapper();
            JsonNode inNode = om.readTree(in.readUTF());
            if (!inNode.isObject()) {
                throw new IOException("Deserialization error");
            }
            profile = (ObjectNode) inNode;
        }

        public ObjectNode getProfile() {
            return profile;
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
     * @param statementRewriter
     * @return the compiled {@code JobSpecification}
     * @throws AsterixException
     * @throws RemoteException
     * @throws AlgebricksException
     * @throws ACIDException
     */
    JobSpecification rewriteCompileQuery(IClusterInfoCollector clusterInfoCollector, MetadataProvider metadataProvider,
            Query query, ICompiledDmlStatement dmlStatement, Map<String, IAObject> statementParameters,
            IStatementRewriter statementRewriter) throws RemoteException, AlgebricksException, ACIDException;

    /**
     * returns the active dataverse for an entity or a statement
     *
     * @param dataverse:
     *            the entity or statement dataverse
     * @return
     *         returns the passed dataverse if not null, the active dataverse otherwise
     */
    String getActiveDataverseName(String dataverse);

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
