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

package org.apache.hyracks.storage.am.lsm.invertedindex.fulltext;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractFullTextFilterEvaluatorFactory implements IFullTextFilterEvaluatorFactory {
    private static final long serialVersionUID = 3535472161323149351L;

    protected final String name;
    protected final FullTextFilterType type;
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected AbstractFullTextFilterEvaluatorFactory(String name, FullTextFilterType type) {
        this.name = name;
        this.type = type;
    }
}
