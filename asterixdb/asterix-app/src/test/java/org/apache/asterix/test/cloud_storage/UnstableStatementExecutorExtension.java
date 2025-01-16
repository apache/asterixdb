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

package org.apache.asterix.test.cloud_storage;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.asterix.app.cc.IStatementExecutorExtension;
import org.apache.asterix.common.api.ExtensionId;
import org.apache.asterix.translator.IStatementExecutorFactory;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.application.IServiceContext;

public class UnstableStatementExecutorExtension implements IStatementExecutorExtension {

    public static final ExtensionId RETRYING_QUERY_TRANSLATOR_EXTENSION_ID =
            new ExtensionId(UnstableStatementExecutorExtension.class.getSimpleName(), 0);

    @Override
    public ExtensionId getId() {
        return RETRYING_QUERY_TRANSLATOR_EXTENSION_ID;
    }

    @Override
    public void configure(List<Pair<String, String>> args, IServiceContext serviceCtx) {

    }

    @Override
    public IStatementExecutorFactory getQueryTranslatorFactory() {
        return null;
    }

    @Override
    public IStatementExecutorFactory getStatementExecutorFactory(ExecutorService executorService) {
        return new UnstableStatementExecutorFactory(executorService);
    }
}
