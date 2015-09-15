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
package org.apache.asterix.aql.expression;

import java.util.Map;

import org.apache.asterix.aql.base.Statement;
import org.apache.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import org.apache.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import org.apache.asterix.common.exceptions.AsterixException;

public class CreateFeedPolicyStatement implements Statement {

    private final String policyName;
    private final String sourcePolicyName;
    private final Map<String, String> properties;
    private final String sourcePolicyFile;
    private final String description;
    private final boolean ifNotExists;

    public CreateFeedPolicyStatement(String policyName, String sourcePolicyName, Map<String, String> properties,
            String description, boolean ifNotExists) {
        this.policyName = policyName;
        this.sourcePolicyName = sourcePolicyName;
        this.properties = properties;
        this.description = description;
        this.ifNotExists = ifNotExists;
        sourcePolicyFile = null;
    }

    public CreateFeedPolicyStatement(String policyName, String sourcePolicyFile, String description, boolean ifNotExists) {
        this.policyName = policyName;
        this.sourcePolicyName = null;
        this.sourcePolicyFile = sourcePolicyFile;
        this.description = description;
        this.properties = null;
        this.ifNotExists = ifNotExists;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.CREATE_FEED_POLICY;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitCreateFeedPolicyStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public String getPolicyName() {
        return policyName;
    }

    public String getSourcePolicyName() {
        return sourcePolicyName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getSourcePolicyFile() {
        return sourcePolicyFile;
    }

    public String getDescription() {
        return description;
    }

}
