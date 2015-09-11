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
package org.apache.hyracks.yarn.common.resources;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class LocalResourceHelper {
    private static LocalResource createLocalResourceFromPath(Configuration config, File path) throws IOException {
        LocalResource lr = Records.newRecord(LocalResource.class);
        URL url = ConverterUtils.getYarnUrlFromPath(FileContext.getFileContext().makeQualified(new Path(path.toURI())));
        lr.setResource(url);
        lr.setVisibility(LocalResourceVisibility.APPLICATION);
        lr.setTimestamp(path.lastModified());
        lr.setSize(path.length());
        return lr;
    }

    public static LocalResource createFileResource(Configuration config, File path) throws IOException {
        LocalResource lr = createLocalResourceFromPath(config, path);
        lr.setType(LocalResourceType.FILE);
        return lr;
    }

    public static LocalResource createArchiveResource(Configuration config, File path) throws IOException {
        LocalResource lr = createLocalResourceFromPath(config, path);
        lr.setType(LocalResourceType.ARCHIVE);
        return lr;
    }
}