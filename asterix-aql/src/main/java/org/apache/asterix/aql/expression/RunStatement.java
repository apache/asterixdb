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

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class RunStatement implements Statement {

    private String system;
    private List<String> parameters;
    private final Identifier dataverseNameFrom;
    private final Identifier datasetNameFrom;
    private final Identifier dataverseNameTo;
    private final Identifier datasetNameTo;

    public RunStatement(String system, List<String> parameters, Identifier dataverseNameFrom, Identifier datasetNameFrom, Identifier dataverseNameTo, Identifier datasetNameTo) {
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
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return null;
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    @Override
    public Kind getKind() {
        return Kind.RUN;
    }

}
