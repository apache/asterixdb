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
package org.apache.asterix.test.base;

import java.io.File;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class RetainLogsRule extends TestWatcher {
    private final File baseDir;
    private final File destDir;
    private long startTime;

    public RetainLogsRule(File baseDir, File destDir) {
        this.baseDir = baseDir;
        this.destDir = destDir;
    }

    public RetainLogsRule(String baseDir, String destDir) {
        this(new File(baseDir), new File(destDir));
    }

    @Override
    protected void starting(Description description) {
        startTime = System.currentTimeMillis();
    }

    @Override
    protected void failed(Throwable e, Description description) {
        File reportDir = new File(destDir, description.getTestClass().getName() + "." + description.getMethodName());
        reportDir.mkdirs();
        try {
            AsterixTestHelper.deepSelectiveCopy(baseDir, reportDir,
                    pathname -> pathname.getName().endsWith("log") &&
                            pathname.lastModified() > startTime);
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }
}
