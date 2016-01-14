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
package org.apache.asterix.external.input.stream;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.asterix.external.api.IInputStreamProvider;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.asterix.external.util.FeedUtils;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.dataflow.std.file.FileSplit;

public class LocalFSInputStreamProvider implements IInputStreamProvider {

    private String expression;
    private boolean isFeed;
    private Path path;
    private File feedLogFile;

    public LocalFSInputStreamProvider(FileSplit[] fileSplits, IHyracksTaskContext ctx,
            Map<String, String> configuration, int partition, String expression, boolean isFeed,
            FileSplit[] feedLogFileSplits) {
        this.expression = expression;
        this.isFeed = isFeed;
        this.path = fileSplits[partition].getLocalFile().getFile().toPath();
        if (feedLogFileSplits != null) {
            this.feedLogFile = FeedUtils
                    .getAbsoluteFileRef(feedLogFileSplits[partition].getLocalFile().getFile().getPath(),
                            feedLogFileSplits[partition].getIODeviceId(), ctx.getIOManager())
                    .getFile();

        }
    }

    @Override
    public AInputStream getInputStream() throws IOException {
        FeedLogManager feedLogManager = null;
        if (isFeed && feedLogFile != null) {
            feedLogManager = new FeedLogManager(feedLogFile);
        }
        return new LocalFileSystemInputStream(path, expression, feedLogManager, isFeed);
    }
}
