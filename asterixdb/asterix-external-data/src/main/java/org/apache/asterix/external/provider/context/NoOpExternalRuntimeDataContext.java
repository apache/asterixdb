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
package org.apache.asterix.external.provider.context;

import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.apache.asterix.external.api.IExternalDataRuntimeContext;
import org.apache.asterix.external.input.filter.NoOpFilterValueEmbedder;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public final class NoOpExternalRuntimeDataContext implements IExternalDataRuntimeContext {
    public static final IExternalDataRuntimeContext INSTANCE = new NoOpExternalRuntimeDataContext();

    private NoOpExternalRuntimeDataContext() {
    }

    @Override
    public IHyracksTaskContext getTaskContext() {
        throw new IllegalAccessError("should not be invoked");
    }

    @Override
    public int getPartition() {
        return -1;
    }

    @Override
    public IExternalFilterValueEmbedder getValueEmbedder() {
        return NoOpFilterValueEmbedder.INSTANCE;
    }

    @Override
    public Supplier<String> getDatasourceNameSupplier() {
        return ExternalDataConstants.EMPTY_STRING;
    }

    @Override
    public LongSupplier getLineNumberSupplier() {
        return ExternalDataConstants.NO_LINES;
    }
}
