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
package org.apache.asterix.external.classad.object.pool;

import java.util.ArrayList;
import java.util.List;

public abstract class Pool<T> {
    protected List<T> inUse = new ArrayList<T>();
    protected int pointer = 0;

    public T get() {
        if (pointer >= inUse.size()) {
            inUse.add(newInstance());
        }
        T t = inUse.get(pointer);
        pointer++;
        reset(t);
        return t;
    }

    public abstract T newInstance();

    public void reset() {
        pointer = 0;
    }

    protected abstract void reset(T obj);
}
