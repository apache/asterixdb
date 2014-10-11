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
import edu.uci.ics.asterix.common.functions.FunctionSignature;

public class CreateFeedStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier feedName;
    private final String adapterName;
    private final Map<String, String> adapterConfiguration;
    private final FunctionSignature appliedFunction;
    private final boolean ifNotExists;

    public CreateFeedStatement(Identifier dataverseName, Identifier feedName, String adapterName,
            Map<String, String> adapterConfiguration, FunctionSignature appliedFunction, boolean ifNotExists) {
        this.feedName = feedName;
        this.dataverseName = dataverseName;
        this.adapterName = adapterName;
        this.adapterConfiguration = adapterConfiguration;
        this.appliedFunction = appliedFunction;
        this.ifNotExists = ifNotExists;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getFeedName() {
        return feedName;
    }

    public String getAdapterName() {
        return adapterName;
    }

    public Map<String, String> getAdapterConfiguration() {
        return adapterConfiguration;
    }

    public FunctionSignature getAppliedFunction() {
        return appliedFunction;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_FEED;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitCreateFeedStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
