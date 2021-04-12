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

package org.apache.asterix.common.utils;

import static org.apache.asterix.common.utils.IdentifierUtil.DATASET;
import static org.apache.asterix.common.utils.IdentifierUtil.DATAVERSE;

import org.apache.asterix.common.api.IIdentifierMapper;
import org.apache.asterix.common.api.IIdentifierMapper.Modifier;

public class IdentifierMappingUtil {

    private static final String SINGULAR_DATASET = "a dataset";
    private static final String PLURAL_DATASET = "datasets";

    private static final String SINGULAR_DATAVERSE = "a dataverse";
    private static final String PLURAL_DATAVERSE = "dataverses";

    private static final IIdentifierMapper DEFAULT_MAPPER = (identifier, modifier) -> {
        switch (identifier) {
            case DATASET:
                switch (modifier) {
                    case NONE:
                        return DATASET;
                    case SINGULAR:
                        return SINGULAR_DATASET;
                    case PLURAL:
                        return PLURAL_DATASET;
                    default:
                        throw new IllegalArgumentException("unknown modifier " + modifier);
                }
            case DATAVERSE:
                switch (modifier) {
                    case NONE:
                        return DATAVERSE;
                    case SINGULAR:
                        return SINGULAR_DATAVERSE;
                    case PLURAL:
                        return PLURAL_DATAVERSE;
                    default:
                        throw new IllegalArgumentException("unknown modifier " + modifier);
                }
            default:
                throw new IllegalArgumentException("unmapped identifier: " + identifier);
        }
    };

    private static IIdentifierMapper mapper = DEFAULT_MAPPER;

    private IdentifierMappingUtil() {
    }

    public static void setMapper(IIdentifierMapper mapper) {
        IdentifierMappingUtil.mapper = mapper;
    }

    public static String map(String key, Modifier modifier) {
        return mapper.map(key, modifier);
    }

}
