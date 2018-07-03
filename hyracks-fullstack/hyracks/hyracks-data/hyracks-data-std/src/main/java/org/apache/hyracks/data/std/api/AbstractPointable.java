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
package org.apache.hyracks.data.std.api;

public abstract class AbstractPointable implements IPointable {

    protected byte[] bytes;

    protected int start;

    protected int length;

    @Override
    public void set(byte[] bytes, int start, int length) {
        this.bytes = bytes;
        this.start = start;
        this.length = length;
        afterReset();
    }

    @Override
    public void set(IValueReference pointer) {
        set(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    /**
     * This method will be called after set the new bytes values.
     * It could be used to reset the state of the inherited Pointable object.
     */
    protected void afterReset() {
    }

    @Override
    public byte[] getByteArray() {
        return bytes;
    }

    @Override
    public int getStartOffset() {
        return start;
    }

    @Override
    public int getLength() {
        return length;
    }
}
