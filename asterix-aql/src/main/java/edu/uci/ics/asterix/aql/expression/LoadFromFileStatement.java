/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.expression;

import java.util.Map;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class LoadFromFileStatement implements Statement {

    private Identifier datasetName;
    private Identifier dataverseName;
    private String adapter;
    private Map<String, String> properties;
    private boolean dataIsLocallySorted;

    public LoadFromFileStatement(Identifier dataverseName, Identifier datasetName, String adapter,
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
        return Kind.LOAD_FROM_FILE;
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public boolean dataIsAlreadySorted() {
        return dataIsLocallySorted;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitLoadFromFileStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
