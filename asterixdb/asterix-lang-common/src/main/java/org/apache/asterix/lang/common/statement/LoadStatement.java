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

import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class LoadStatement extends AbstractStatement {

    private Identifier datasetName;
    private Identifier dataverseName;
    private String adapter;
    private Map<String, String> properties;
    private boolean dataIsLocallySorted;

    public LoadStatement(Identifier dataverseName, Identifier datasetName, String adapter,
            Map<String, String> propertiees, boolean dataIsLocallySorted) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.adapter = adapter;
        this.properties = propertiees;
        this.dataIsLocallySorted = dataIsLocallySorted;
    }

    public String getAdapter() {
        return adapter;
    }

    public void setAdapter(String adapter) {
        this.adapter = adapter;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public void setDataverseName(Identifier dataverseName) {
        this.dataverseName = dataverseName;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.LOAD;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public boolean dataIsAlreadySorted() {
        return dataIsLocallySorted;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.UPDATE;
    }

}
