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
package org.apache.asterix.transaction.management.resource;

import org.apache.asterix.common.transactions.IResourceFactory;
import org.apache.hyracks.storage.common.file.ILocalResourceFactory;
import org.apache.hyracks.storage.common.file.ILocalResourceFactoryProvider;

public class PersistentLocalResourceFactoryProvider implements ILocalResourceFactoryProvider {

    private static final long serialVersionUID = 1L;
    private final IResourceFactory factory;
    private final int type;

    public PersistentLocalResourceFactoryProvider(IResourceFactory factory, int type) {
        this.factory = factory;
        this.type = type;
    }

    @Override
    public ILocalResourceFactory getLocalResourceFactory() {
        return new PersistentLocalResourceFactory(factory, type);
    }
}
