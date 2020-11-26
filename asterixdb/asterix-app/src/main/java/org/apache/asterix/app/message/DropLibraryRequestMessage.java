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
package org.apache.asterix.app.message;

import java.util.Map;

import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.statement.LibraryDropStatement;

public final class DropLibraryRequestMessage extends AbstractInternalRequestMessage {

    final DataverseName dataverseName;
    final String libraryName;
    final boolean ifExists;
    private static final long serialVersionUID = 1L;

    public DropLibraryRequestMessage(String nodeRequestId, long requestMessageId, DataverseName dataverseName,
            String libraryName, boolean ifExists, IRequestReference requestReference,
            Map<String, String> additionalParams) {
        super(nodeRequestId, requestMessageId, requestReference, additionalParams);
        this.dataverseName = dataverseName;
        this.libraryName = libraryName;
        this.ifExists = ifExists;
    }

    @Override
    protected Statement produceStatement() {
        return new LibraryDropStatement(dataverseName, libraryName, ifExists);
    }
}
