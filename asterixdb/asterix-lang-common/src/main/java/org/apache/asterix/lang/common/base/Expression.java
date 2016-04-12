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
package org.apache.asterix.lang.common.base;

public interface Expression extends ILangExpression {
    public abstract Kind getKind();

    public enum Kind {
        LITERAL_EXPRESSION,
        FLWOGR_EXPRESSION,
        IF_EXPRESSION,
        QUANTIFIED_EXPRESSION,
        // PARENTHESIZED_EXPRESSION,
        LIST_CONSTRUCTOR_EXPRESSION,
        RECORD_CONSTRUCTOR_EXPRESSION,
        VARIABLE_EXPRESSION,
        METAVARIABLE_EXPRESSION,
        CALL_EXPRESSION,
        OP_EXPRESSION,
        FIELD_ACCESSOR_EXPRESSION,
        INDEX_ACCESSOR_EXPRESSION,
        UNARY_EXPRESSION,
        UNION_EXPRESSION,
        SELECT_EXPRESSION,
        PRIMARY_EXPRESSION,
        VALUE_EXPRESSION
    }

}
