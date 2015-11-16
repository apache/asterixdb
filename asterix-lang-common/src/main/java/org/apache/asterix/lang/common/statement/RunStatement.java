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

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class RunStatement implements Statement {

    private String system;
    private List<String> parameters;
    private final Identifier dataverseNameFrom;
    private final Identifier datasetNameFrom;
    private final Identifier dataverseNameTo;
    private final Identifier datasetNameTo;

    public RunStatement(String system, List<String> parameters, Identifier dataverseNameFrom,
            Identifier datasetNameFrom, Identifier dataverseNameTo, Identifier datasetNameTo) {
        this.system = system;
        this.parameters = parameters;
        this.datasetNameFrom = datasetNameFrom;
        this.dataverseNameFrom = dataverseNameFrom;
        this.datasetNameTo = datasetNameTo;
        this.dataverseNameTo = dataverseNameTo;
    }

    public String getSystem() {
        return system;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public Identifier getDataverseNameFrom() {
        return dataverseNameFrom;
    }

    public Identifier getDatasetNameFrom() {
        return datasetNameFrom;
    }

    public Identifier getDataverseNameTo() {
        return dataverseNameTo;
    }

    public Identifier getDatasetNameTo() {
        return datasetNameTo;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    @Override
    public Kind getKind() {
        return Kind.RUN;
    }

}
