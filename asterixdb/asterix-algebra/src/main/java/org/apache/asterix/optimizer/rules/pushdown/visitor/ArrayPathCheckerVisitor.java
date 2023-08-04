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
package org.apache.asterix.optimizer.rules.pushdown.visitor;

import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.EMPTY_TYPE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.column.filter.iterable.evaluator.ColumnarRepeatedIterableFilterEvaluator;
import org.apache.asterix.om.typecomputer.impl.RecordMergeTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.IATypeVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

/**
 * TODO this should be removed once we support rewindable readers
 *
 * @see ColumnarRepeatedIterableFilterEvaluator#evaluate()
 */
public class ArrayPathCheckerVisitor implements IATypeVisitor<Boolean, AbstractCollectionType> {
    private final Set<AbstractCollectionType> seenCollections;
    private boolean firstPath;

    public ArrayPathCheckerVisitor() {
        seenCollections = new HashSet<>();
    }

    public boolean containsMultipleArrayPaths(Collection<ARecordType> paths) throws AlgebricksException {
        ARecordType mergedPaths = EMPTY_TYPE;
        for (ARecordType path : paths) {
            mergedPaths = (ARecordType) RecordMergeTypeComputer.merge(mergedPaths, path);
        }
        firstPath = true;
        return !mergedPaths.accept(this, null);
    }

    @Override
    public Boolean visit(ARecordType recordType, AbstractCollectionType arg) {
        for (IAType child : recordType.getFieldTypes()) {
            if (!child.accept(this, null)) {
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visit(AbstractCollectionType collectionType, AbstractCollectionType arg) {
        if (firstPath) {
            seenCollections.add(collectionType);
        } else if (!seenCollections.contains(collectionType)) {
            return Boolean.FALSE;
        }
        return collectionType.getItemType().accept(this, collectionType);
    }

    @Override
    public Boolean visit(AUnionType unionType, AbstractCollectionType arg) {
        for (IAType child : unionType.getUnionList()) {
            if (!child.accept(this, arg)) {
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    @Override
    public Boolean visitFlat(IAType flatType, AbstractCollectionType arg) {
        if (arg != null) {
            firstPath = false;
        }
        return Boolean.TRUE;
    }
}
