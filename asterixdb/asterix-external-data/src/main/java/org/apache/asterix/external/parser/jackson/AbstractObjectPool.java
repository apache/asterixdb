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

import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.asterix.om.util.container.IObjectFactory;

/**
 * Object pool for DFS traversal mode, which allows to recycle objects
 * as soon as it is not needed.
 */
public abstract class AbstractObjectPool<E, T, Q> implements IObjectPool<E> {
    private final IObjectFactory<E, T> objectFactory;
    private final Queue<Q> recycledObjects;
    private final T param;

    protected AbstractObjectPool(IObjectFactory<E, T> objectFactory, T param) {
        this.objectFactory = objectFactory;
        recycledObjects = new ArrayDeque<>();
        this.param = param;
    }

    public E getInstance() {
        E instance = unwrap(recycledObjects.poll());
        if (objectFactory != null && instance == null) {
            instance = objectFactory.create(param);
        }
        return instance;
    }

    public void recycle(E object) {
        if (object != null) {
            recycledObjects.add(wrap(object));
        }
    }

    protected abstract E unwrap(Q element);

    protected abstract Q wrap(E element);

    @Override
    public String toString() {
        return recycledObjects.toString();
    }
}
