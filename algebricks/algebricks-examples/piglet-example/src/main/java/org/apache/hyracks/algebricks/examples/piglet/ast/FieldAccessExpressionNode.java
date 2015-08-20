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
package edu.uci.ics.hyracks.algebricks.examples.piglet.ast;

public class FieldAccessExpressionNode extends ExpressionNode {
    private String relationName;

    private String fieldName;

    public FieldAccessExpressionNode(String relationName, String fieldName) {
        this.relationName = relationName;
        this.fieldName = fieldName;
    }

    @Override
    public Tag getTag() {
        return Tag.FIELD_ACCESS;
    }

    public String getRelationName() {
        return relationName;
    }

    public String getFieldName() {
        return fieldName;
    }
}