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

package org.apache.hyracks.client.stats;

public class Counters {
    public static final String NUM_PROCESSOR = "num-processors";

    public static final String SYSTEM_LOAD = "system-load-averages";

    public static final String MEMORY_USAGE = "heap-used-sizes";

    public static final String MEMORY_MAX = "heap-max-sizes";

    public static final String NETWORK_IO_READ = "net-payload-bytes-read";

    public static final String NETWORK_IO_WRITE = "net-payload-bytes-written";

    public static final String DISK_READ = "disk-reads";

    public static final String DISK_WRITE = "disk-writes";
}
