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
package org.apache.asterix.lang.common.statement;

import java.util.List;

import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.expression.VariableExpr;

public class ChannelUnsubscribeStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier channelName;
    private final String subscriptionId;
    private final int varCounter;
    private VariableExpr vars;
    private List<String> dataverses;
    private List<String> datasets;

    public ChannelUnsubscribeStatement(VariableExpr vars, Identifier dataverseName, Identifier channelName,
            String subscriptionId, int varCounter, List<String> dataverses, List<String> datasets) {
        this.vars = vars;
        this.channelName = channelName;
        this.dataverseName = dataverseName;
        this.subscriptionId = subscriptionId;
        this.varCounter = varCounter;
        this.dataverses = dataverses;
        this.datasets = datasets;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public VariableExpr getVariableExpr() {
        return vars;
    }

    public Identifier getChannelName() {
        return channelName;
    }

    public String getsubScriptionId() {
        return subscriptionId;
    }

    public List<String> getDataverses() {
        return dataverses;
    }

    public List<String> getDatasets() {
        return datasets;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public Kind getKind() {
        return Kind.UNSUBSCRIBE_CHANNEL;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitChannelUnsubscribeStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }
}