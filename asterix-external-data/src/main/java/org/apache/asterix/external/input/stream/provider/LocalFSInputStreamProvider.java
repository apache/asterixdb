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
package org.apache.asterix.external.input.stream.provider;

import java.nio.file.Path;
import java.util.Map;

import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.input.stream.AInputStream;
import org.apache.asterix.external.input.stream.LocalFileSystemInputStream;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.FileSplit;

public class LocalFSInputStreamProvider implements IInputStreamProvider {

    private final String expression;
    private final boolean isFeed;
    private final Path path;
    private FeedLogManager feedLogManager;

    public LocalFSInputStreamProvider(final FileSplit[] fileSplits, final IHyracksTaskContext ctx,
            final Map<String, String> configuration, final int partition, final String expression,
            final boolean isFeed) {
        this.expression = expression;
        this.isFeed = isFeed;
        this.path = fileSplits[partition].getLocalFile().getFile().toPath();
    }

    @Override
    public AInputStream getInputStream() throws HyracksDataException {
        final LocalFileSystemInputStream stream = new LocalFileSystemInputStream(path, expression, isFeed);
        stream.setFeedLogManager(feedLogManager);
        return stream;
    }

    @Override
    public void setFeedLogManager(final FeedLogManager feedLogManager) {
        this.feedLogManager = feedLogManager;
    }
}
