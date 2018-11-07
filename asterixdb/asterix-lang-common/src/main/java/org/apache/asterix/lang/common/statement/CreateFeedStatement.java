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
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.struct.Identifier;
import org.apache.asterix.lang.common.util.ConfigurationUtil;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;

/**
 * The new create feed statement only concerns the feed adaptor configuration.
 * All feeds are considered as primary feeds.
 */
public class CreateFeedStatement extends AbstractStatement {

    private final Pair<Identifier, Identifier> qName;
    private final boolean ifNotExists;
    private final AdmObjectNode withObjectNode;

    public CreateFeedStatement(Pair<Identifier, Identifier> qName, RecordConstructor withRecord, boolean ifNotExists)
            throws AlgebricksException {
        this.qName = qName;
        this.ifNotExists = ifNotExists;
        this.withObjectNode = withRecord == null ? null : ExpressionUtils.toNode(withRecord);
    }

    public Identifier getDataverseName() {
        return qName.first;
    }

    public Identifier getFeedName() {
        return qName.second;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_FEED;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public Map<String, String> getConfiguration() throws CompilationException {
        return ConfigurationUtil.toProperties(withObjectNode);
    }

    public AdmObjectNode getWithObjectNode() {
        return withObjectNode;
    }
}
