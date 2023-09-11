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
package org.apache.asterix.external.input.filter;

import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public final class NoOpFilterValueEmbedder implements IExternalFilterValueEmbedder {
    public static final IExternalFilterValueEmbedder INSTANCE = new NoOpFilterValueEmbedder();

    private NoOpFilterValueEmbedder() {
    }

    @Override
    public void reset() {
        // NoOp
    }

    @Override
    public void setPath(String path) throws HyracksDataException {
        // NoOp
    }

    @Override
    public boolean shouldEmbed(String fieldName, ATypeTag typeTag) {
        return false;
    }

    @Override
    public IValueReference getEmbeddedValue() {
        throw new IllegalAccessError("Cannot embed a value to " + this.getClass().getName());
    }

    @Override
    public boolean isMissingEmbeddedValues() {
        return false;
    }

    @Override
    public boolean isMissing(String fieldName) {
        return false;
    }

    @Override
    public String[] getEmbeddedFieldNames() {
        return null;
    }

    @Override
    public void enterObject() {
        // NoOp
    }

    @Override
    public void exitObject() {
        // NoOp
    }
}
