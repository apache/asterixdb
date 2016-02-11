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
package @PACKAGE@;

public class RecordManagerStats {
    int arenas  = 0;
    int buffers = 0;
    int slots   = 0;
    int items   = 0;
    int size    = 0;

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ arenas : ").append(arenas);
        sb.append(", buffers : ").append(buffers);
        sb.append(", slots : ").append(slots);
        sb.append(", items : ").append(items);
        sb.append(", size : ").append(size).append(" }");
        return sb.toString();
    }
}
