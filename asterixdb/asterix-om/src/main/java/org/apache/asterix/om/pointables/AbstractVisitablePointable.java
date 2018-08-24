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

package org.apache.asterix.om.pointables;

import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.hyracks.data.std.api.IValueReference;

/**
 * This class implements several "routine" methods in IVisitablePointable
 * interface, so that subclasses do not need to repeat the same code.
 */
public abstract class AbstractVisitablePointable implements IVisitablePointable {

    private byte[] data;
    private int start = -1;
    private int len = -1;

    @Override
    public byte[] getByteArray() {
        return data;
    }

    @Override
    public int getLength() {
        return len;
    }

    @Override
    public int getStartOffset() {
        return start;
    }

    @Override
    public void set(byte[] b, int start, int len) {
        this.data = b;
        this.start = start;
        this.len = len;
    }

    @Override
    public void set(IValueReference ivf) {
        set(ivf.getByteArray(), ivf.getStartOffset(), ivf.getLength());
    }

    @Override
    public String toString() {
        return "{ \"class\" : \"" + getClass().getSimpleName() + "\", \"data\" : "
                + (data == null ? "null" : ("\"" + System.identityHashCode(data) + ":" + data.length + "\""))
                + ", \"offset\" : " + start + ", \"length\" : " + len + " }";
    }
}
