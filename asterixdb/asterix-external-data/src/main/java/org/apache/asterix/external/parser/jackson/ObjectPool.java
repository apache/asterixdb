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
public class ObjectPool<E, T> {
    private final IObjectFactory<E, T> objectFactory;
    private final Queue<E> recycledObjects;
    private final T element;

    public ObjectPool() {
        this(null, null);
    }

    public ObjectPool(IObjectFactory<E, T> objectFactory) {
        this(objectFactory, null);
    }

    public ObjectPool(IObjectFactory<E, T> objectFactory, T element) {
        this.objectFactory = objectFactory;
        recycledObjects = new ArrayDeque<>();
        this.element = element;
    }

    public E getInstance() {
        E instance = recycledObjects.poll();
        if (objectFactory != null && instance == null) {
            instance = objectFactory.create(element);
        }
        return instance;
    }

    public void recycle(E object) {
        if (object != null) {
            recycledObjects.add(object);
        }
    }

    @Override
    public String toString() {
        return recycledObjects.toString();
    }
}
