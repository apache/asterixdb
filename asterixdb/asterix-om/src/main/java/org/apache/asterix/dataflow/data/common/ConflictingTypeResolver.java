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

package org.apache.asterix.dataflow.data.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;

/**
 * The AsterixDB implementation for IConflictingTypeResolver.
 */
public class ConflictingTypeResolver implements IConflictingTypeResolver {

    public static final ConflictingTypeResolver INSTANCE = new ConflictingTypeResolver();

    private ConflictingTypeResolver() {
    }

    @Override
    public Object resolve(Object... inputTypes) {
        List<IAType> types = new ArrayList<>();
        for (Object object : inputTypes) {
            types.add((IAType) object);
        }
        return TypeResolverUtil.resolve(types);
    }

}
