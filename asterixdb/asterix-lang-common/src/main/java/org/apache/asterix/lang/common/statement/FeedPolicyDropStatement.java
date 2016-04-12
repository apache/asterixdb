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

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class FeedPolicyDropStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier policyName;
    private boolean ifExists;

    public FeedPolicyDropStatement(Identifier dataverseName, Identifier policyName, boolean ifExists) {
        this.dataverseName = dataverseName;
        this.policyName = policyName;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.DROP_FEED_POLICY;
    }

    public Identifier getDataverseName() {
        return dataverseName;
    }

    public Identifier getPolicyName() {
        return policyName;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

}
