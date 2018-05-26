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
package org.apache.asterix.algebra.extension;

import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.translator.IRequestParameters;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An interface that provides an extension mechanism to extend a language with additional statements
 */
public abstract class ExtensionStatement extends AbstractStatement {

    @Override
    public final Kind getKind() {
        return Kind.EXTENSION;
    }

    /**
     * Called when the {@code IStatementExecutor} encounters an extension statement.
     * An implementation class should implement the actual processing of the statement in this method.
     *
     * @param hcc
     * @param statementExecutor
     * @param requestParameters
     * @param metadataProvider
     * @param resultSetId
     * @throws HyracksDataException
     * @throws AlgebricksException
     */
    public abstract void handle(IHyracksClientConnection hcc, IStatementExecutor statementExecutor,
            IRequestParameters requestParameters, MetadataProvider metadataProvider, int resultSetId)
            throws HyracksDataException, AlgebricksException;
}