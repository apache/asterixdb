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

package org.apache.asterix.common.metadata;

import java.util.List;

import org.apache.asterix.common.api.INamespaceResolver;
import org.apache.asterix.common.exceptions.AsterixException;

public class NamespaceResolver implements INamespaceResolver {

    private final boolean usingDatabase;

    public NamespaceResolver(boolean usingDatabase) {
        this.usingDatabase = usingDatabase;
    }

    @Override
    public Namespace resolve(List<String> multiIdentifier) throws AsterixException {
        return resolve(multiIdentifier, 0, multiIdentifier.size());
    }

    @Override
    public Namespace resolve(List<String> multiIdentifier, int fromIndex, int toIndex) throws AsterixException {
        if (multiIdentifier == null) {
            return null;
        }
        if (usingDatabase) {
            int partsNum = toIndex - fromIndex;
            if (partsNum > 1) {
                String databaseName = multiIdentifier.get(fromIndex);
                return ofDatabase(databaseName, multiIdentifier, fromIndex + 1, toIndex);
            } else {
                return ofDataverse(multiIdentifier, fromIndex, toIndex);
            }
        } else {
            return ofDataverse(multiIdentifier, fromIndex, toIndex);
        }
    }

    @Override
    public Namespace resolve(String namespace) throws AsterixException {
        DataverseName dataverseName = DataverseName.createFromCanonicalForm(namespace);
        if (usingDatabase) {
            List<String> parts = dataverseName.getParts();
            if (parts.size() > 1) {
                String databaseName = parts.get(0);
                return ofDatabase(databaseName, parts, 1, parts.size());
            } else {
                return new Namespace(MetadataUtil.databaseFor(dataverseName), dataverseName);
            }
        } else {
            return new Namespace(MetadataUtil.databaseFor(dataverseName), dataverseName);
        }
    }

    @Override
    public boolean isUsingDatabase() {
        return usingDatabase;
    }

    private static Namespace ofDatabase(String databaseName, List<String> multiIdentifier, int fromIndex, int toIndex)
            throws AsterixException {
        DataverseName dataverseName = DataverseName.create(multiIdentifier, fromIndex, toIndex);
        return new Namespace(databaseName, dataverseName);
    }

    private static Namespace ofDataverse(List<String> multiIdentifier, int fromIndex, int toIndex)
            throws AsterixException {
        DataverseName dataverseName = DataverseName.create(multiIdentifier, fromIndex, toIndex);
        return new Namespace(MetadataUtil.databaseFor(dataverseName), dataverseName);
    }
}
