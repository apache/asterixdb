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
package org.apache.asterix.external.library.java.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.asterix.external.api.IJObject;

public abstract class JList implements IJObject {
    protected List<IJObject> jObjects;

    public JList() {
        jObjects = new ArrayList<>();
    }

    public boolean isEmpty() {
        return jObjects.isEmpty();
    }

    public void add(IJObject jObject) {
        jObjects.add(jObject);
    }

    public void addAll(Collection<IJObject> jObjectCollection) {
        jObjects.addAll(jObjectCollection);
    }

    public void clear() {
        jObjects.clear();
    }

    public IJObject getElement(int index) {
        return jObjects.get(index);
    }

    public int size() {
        return jObjects.size();
    }

    public Iterator<IJObject> iterator() {
        return jObjects.iterator();
    }
}
