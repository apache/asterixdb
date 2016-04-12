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
package org.apache.asterix.event.driver;

import org.kohsuke.args4j.Option;

public class EventConfig {

    @Option(name = "-h", required = false, usage = "Help")
    public boolean help = false;

    @Option(name = "-d", required = false, usage = "Show the execution on a timeline")
    public boolean dryRun = false;

    @Option(name = "-s", required = false, usage = "Seed for randomization")
    public int seed = -1;

    @Option(name = "-c", required = true, usage = "Path to cluster configuration (REQUIRED)")
    public String clusterPath;

    @Option(name = "-p", required = true, usage = "Path to pattern configuration (REQUIRED)")
    public String patternPath;

}
