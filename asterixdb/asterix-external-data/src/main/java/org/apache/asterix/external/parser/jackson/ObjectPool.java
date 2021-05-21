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
package org.apache.asterix.external.parser.jackson;

import org.apache.asterix.om.util.container.IObjectFactory;

/**
 * Object pool for DFS traversal mode, which allows to recycle objects
 * as soon as it is not needed.
 */
public class ObjectPool<E, T> extends AbstractObjectPool<E, T, E> {
    public ObjectPool() {
        this(null, null);
    }

    public ObjectPool(IObjectFactory<E, T> objectFactory) {
        this(objectFactory, null);
    }

    public ObjectPool(IObjectFactory<E, T> objectFactory, T param) {
        super(objectFactory, param);
    }

    @Override
    protected E unwrap(E wrapped) {
        return wrapped;
    }

    @Override
    protected E wrap(E element) {
        return element;
    }
}
