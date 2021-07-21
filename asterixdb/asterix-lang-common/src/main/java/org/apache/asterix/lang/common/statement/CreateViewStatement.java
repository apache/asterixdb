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

import java.util.Objects;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.expression.TypeExpression;
import org.apache.asterix.lang.common.util.ViewUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.object.base.AdmObjectNode;

public final class CreateViewStatement extends AbstractStatement {

    private final DataverseName dataverseName;

    private final String viewName;

    private final TypeExpression itemType;

    private final String viewBody;

    private final Expression viewBodyExpression;

    private final AdmObjectNode withObjectNode;

    private final Boolean defaultNull;

    private final boolean replaceIfExists;

    private final boolean ifNotExists;

    public CreateViewStatement(DataverseName dataverseName, String viewName, TypeExpression itemType, String viewBody,
            Expression viewBodyExpression, Boolean defaultNull, RecordConstructor withRecord, boolean replaceIfExists,
            boolean ifNotExists) throws CompilationException {
        this.dataverseName = dataverseName;
        this.viewName = Objects.requireNonNull(viewName);
        this.itemType = itemType;
        this.viewBody = Objects.requireNonNull(viewBody);
        this.viewBodyExpression = Objects.requireNonNull(viewBodyExpression);
        this.defaultNull = defaultNull;
        this.withObjectNode = ViewUtil.validateAndGetWithObjectNode(withRecord, itemType != null);
        this.replaceIfExists = replaceIfExists;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Statement.Kind.CREATE_VIEW;
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    public String getViewName() {
        return viewName;
    }

    public boolean hasItemType() {
        return itemType != null;
    }

    public TypeExpression getItemType() {
        return itemType;
    }

    public String getViewBody() {
        return viewBody;
    }

    public Expression getViewBodyExpression() {
        return viewBodyExpression;
    }

    public boolean getReplaceIfExists() {
        return replaceIfExists;
    }

    public boolean getIfNotExists() {
        return ifNotExists;
    }

    // Typed view parameters

    public Boolean getDefaultNull() {
        return defaultNull;
    }

    public String getDatetimeFormat() {
        return withObjectNode.getOptionalString(ViewUtil.DATETIME_PARAMETER_NAME);
    }

    public String getDateFormat() {
        return withObjectNode.getOptionalString(ViewUtil.DATE_PARAMETER_NAME);
    }

    public String getTimeFormat() {
        return withObjectNode.getOptionalString(ViewUtil.TIME_PARAMETER_NAME);
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }
}
