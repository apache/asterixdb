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

package org.apache.asterix.runtime.fulltext;

import java.util.Objects;

import org.apache.asterix.common.metadata.DataverseName;

public abstract class AbstractFullTextFilterDescriptor implements IFullTextFilterDescriptor {
    private static final long serialVersionUID = 5325972301942118022L;

    private final String databaseName;
    protected final DataverseName dataverseName;
    protected final String name;

    public AbstractFullTextFilterDescriptor(String databaseName, DataverseName dataverseName, String name) {
        this.databaseName = Objects.requireNonNull(databaseName);
        this.dataverseName = dataverseName;
        this.name = name;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public DataverseName getDataverseName() {
        return dataverseName;
    }

    @Override
    public String getName() {
        return name;
    }
}
