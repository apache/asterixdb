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
package org.apache.asterix.lang.aql.clause;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.lang.aql.visitor.base.IAQLPlusVisitor;
import org.apache.asterix.lang.common.base.AbstractClause;
import org.apache.asterix.lang.common.struct.VarIdentifier;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class MetaVariableClause extends AbstractClause {
    private VarIdentifier var;

    public MetaVariableClause() {
    }

    public MetaVariableClause(VarIdentifier var) {
        this.var = var;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return ((IAQLPlusVisitor<R, T>) visitor).visitMetaVariableClause(this, arg);
    }

    @Override
    public ClauseType getClauseType() {
        return null;
    }

    public VarIdentifier getVar() {
        return var;
    }

    public void setVar(VarIdentifier var) {
        this.var = var;
    }
}
