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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.input.stream.AInputStream;
import org.apache.asterix.external.input.stream.LocalFileSystemInputStream;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.std.file.FileSplit;

public class LocalFSInputStreamProvider implements IInputStreamProvider {

    private String expression;
    private boolean isFeed;
    private Path path;
    private FeedLogManager feedLogManager;
    private Map<String, String> configuration;

    public LocalFSInputStreamProvider(FileSplit[] fileSplits, IHyracksTaskContext ctx,
            Map<String, String> configuration, int partition, String expression, boolean isFeed) {
        this.expression = expression;
        this.isFeed = isFeed;
        this.path = fileSplits[partition].getLocalFile().getFile().toPath();
    }

    @Override
    public AInputStream getInputStream() throws IOException {
        LocalFileSystemInputStream stream = new LocalFileSystemInputStream(path, expression, isFeed);
        stream.setFeedLogManager(feedLogManager);
        stream.configure(configuration);
        return stream;
    }

    @Override
    public void configure(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) {
        this.feedLogManager = feedLogManager;
    }
}
