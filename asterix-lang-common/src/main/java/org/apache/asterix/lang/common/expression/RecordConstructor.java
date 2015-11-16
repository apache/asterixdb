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
package org.apache.asterix.lang.common.expression;

import java.util.List;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;

public class RecordConstructor implements Expression {
    private List<FieldBinding> fbList;

    public RecordConstructor() {
        super();
    }

    public RecordConstructor(List<FieldBinding> fbList) {
        super();
        this.fbList = fbList;
    }

    public List<FieldBinding> getFbList() {
        return fbList;
    }

    public void setFbList(List<FieldBinding> fbList) {
        this.fbList = fbList;
    }

    @Override
    public Kind getKind() {
        return Kind.RECORD_CONSTRUCTOR_EXPRESSION;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }
}
