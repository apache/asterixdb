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

import java.rmi.RemoteException;

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.statement.Query;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.CompiledStatements.ICompiledDmlStatement;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.api.job.JobSpecification;

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

    public static class Stats {
        private long count;
        private long size;

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

    }

    /**
     * Compiles and execute a list of statements.
     *
     * @param hcc
     *            A Hyracks client connection that is used to submit a jobspec to Hyracks.
     * @param hdc
     *            A Hyracks dataset client object that is used to read the results.
     * @param resultDelivery
     *            The {@code ResultDelivery} kind required for queries in the list of statements
     * @throws Exception
     */
    void compileAndExecute(IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery)
            throws Exception;

    /**
     * Compiles and execute a list of statements.
     *
     * @param hcc
     *            A Hyracks client connection that is used to submit a jobspec to Hyracks.
     * @param hdc
     *            A Hyracks dataset client object that is used to read the results.
     * @param resultDelivery
     *            The {@code ResultDelivery} kind required for queries in the list of statements
     * @param stats
     *            a reference to write the stats of executed queries
     * @throws Exception
     */
    void compileAndExecute(IHyracksClientConnection hcc, IHyracksDataset hdc, ResultDelivery resultDelivery,
            Stats stats) throws Exception;

    /**
     * rewrites and compiles query into a hyracks job specifications
     *
     * @param metadataProvider
     *            The metadataProvider used to access metadata and build runtimes
     * @param query
     *            The query to be compiled
     * @param dmlStatement
     *            The data modification statement when the query results in a modification to a dataset
     * @return the compiled {@code JobSpecification}
     * @throws AsterixException
     * @throws RemoteException
     * @throws AlgebricksException
     * @throws ACIDException
     */
    JobSpecification rewriteCompileQuery(MetadataProvider metadataProvider, Query query,
            ICompiledDmlStatement dmlStatement)
                    throws AsterixException, RemoteException, AlgebricksException, ACIDException;

    /**
     * returns the active dataverse for an entity or a statement
     *
     * @param dataverse:
     *            the entity or statement dataverse
     * @return
     *         returns the passed dataverse if not null, the active dataverse otherwise
     */
    String getActiveDataverseName(String dataverse);

}
