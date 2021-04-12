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

package org.apache.asterix.lang.common.statement;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class CreateSynonymStatement extends AbstractStatement {

    private final DataverseName dataverseName;

    private final String synonymName;

    private final DataverseName objectDataverseName;

    private final String objectName;

    private final boolean ifNotExists;

    public CreateSynonymStatement(DataverseName dataverseName, String synonymName, DataverseName objectDataverseName,
            String objectName, boolean ifNotExists) {
        this.dataverseName = dataverseName;
        this.synonymName = synonymName;
        this.objectDataverseName = objectDataverseName;
        this.objectName = objectName;
        this.ifNotExists = ifNotExists;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getSynonymName() {
        return synonymName;
    }

    public DataverseName getObjectDataverseName() {
        return objectDataverseName;
    }

    public String getObjectName() {
        return objectName;
    }

    public boolean getIfNotExists() {
        return ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_SYNONYM;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }
}
