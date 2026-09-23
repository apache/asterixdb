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
package org.apache.asterix.external.util.google;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Collections;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

/**
 * Hadoop's own defaults can name a gs:// filesystem class that is not on the classpath, and carry fs.gs.* values the
 * bundled connector cannot parse (Hadoop 3.5.0 does both, for its hadoop-gcp module). Initializing the filesystem
 * parses that configuration and resolves the credentials, so it fails on either, and on an anonymous configuration
 * that falls through to the compute engine metadata server.
 */
public class GCSHadoopFileSystemTest {

    @Test(timeout = 60_000)
    public void testAnonymousGcsFileSystemInitializes() throws Exception {
        JobConf jobConf = new JobConf();
        GCSUtils.configureHdfsJobConf(jobConf, Collections.emptyMap());

        URI bucket = new URI(GCSConstants.HADOOP_GCS_PROTOCOL + "://asterixdb-test-bucket/");
        try (FileSystem fs = FileSystem.newInstance(bucket, jobConf)) {
            assertEquals(bucket, fs.getUri().resolve("/"));
        }
    }
}
