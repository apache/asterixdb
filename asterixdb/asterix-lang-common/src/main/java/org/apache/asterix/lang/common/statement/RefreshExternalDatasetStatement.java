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
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class RefreshExternalDatasetStatement extends AbstractStatement {

    private Identifier dataverseName;
    private Identifier datasetName;

    public Identifier getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(Identifier datasetName) {
        this.datasetName = datasetName;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public void setDataverseName(Identifier dataverseName) {
        this.dataverseName = dataverseName;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return null;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.EXTERNAL_DATASET_REFRESH;
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

}
