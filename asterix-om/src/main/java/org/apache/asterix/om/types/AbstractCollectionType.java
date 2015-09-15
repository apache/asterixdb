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
package org.apache.asterix.om.types;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.visitors.IOMVisitor;

public abstract class AbstractCollectionType extends AbstractComplexType {

    private static final long serialVersionUID = 1L;

    protected IAType itemType;

    AbstractCollectionType(IAType itemType, String typeName) {
        super(typeName);
        this.itemType = itemType;
    }

    public boolean isTyped() {
        return itemType != null;
    }

    public IAType getItemType() {
        return itemType;
    }

    public void setItemType(IAType itemType) {
        this.itemType = itemType;
    }

    @Override
    public IAType getType() {
        return BuiltinType.ASTERIX_TYPE;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAType(this);
    }

    @Override
    public void generateNestedDerivedTypeNames() {
        if (itemType.getTypeTag().isDerivedType() && itemType.getTypeName() == null) {
            AbstractComplexType nestedType = ((AbstractComplexType) itemType);
            nestedType.setTypeName(getTypeName() + "_Item");
            nestedType.generateNestedDerivedTypeNames();
        }
    }
    // public void serialize(DataOutput out) throws IOException {
    // out.writeBoolean(isTyped());
    // }

}
