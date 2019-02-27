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
package org.apache.hyracks.algebricks.data;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;

/**
 * Provides {@link org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory} for different types. Whether a
 * factory is stateful or stateless is implementation-specific. Also, whether a new factory is created each time or a
 * single instance factory is returned is implementation-specific. Therefore, no assumptions should be made about
 * these two aspects.
 */
public interface IBinaryComparatorFactoryProvider {

    /**
     * @param type the type of the left binary data
     * @param ascending the order direction. true if ascending order is desired, false otherwise
     * @return the appropriate {@link org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory} instance
     * @throws AlgebricksException if the comparator factory for the passed type could not be created
     */
    IBinaryComparatorFactory getBinaryComparatorFactory(Object type, boolean ascending) throws AlgebricksException;

    /**
     * @param type the type of the left binary data
     * @param ascending the order direction. true if ascending order is desired, false otherwise
     * @param ignoreCase ignore case for strings
     * @return the appropriate {@link org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory} instance
     * @throws AlgebricksException if the comparator factory for the passed type could not be created
     */
    IBinaryComparatorFactory getBinaryComparatorFactory(Object type, boolean ascending, boolean ignoreCase)
            throws AlgebricksException;

    /**
     * @param leftType the type of the left binary data
     * @param rightType the type of the right binary data
     * @param ascending the order direction. true if ascending order is desired, false otherwise
     * @return the appropriate {@link org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory} instance
     * @throws AlgebricksException if the comparator factory for the passed type could not be created
     */
    IBinaryComparatorFactory getBinaryComparatorFactory(Object leftType, Object rightType, boolean ascending)
            throws AlgebricksException;

    /**
     * @param leftType the type of the left binary data
     * @param rightType the type of the right binary data
     * @param ascending the order direction. true if ascending order is desired, false otherwise
     * @param ignoreCase ignore case for strings
     * @return the appropriate {@link org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory} instance
     * @throws AlgebricksException if the comparator factory for the passed type could not be created
     */
    IBinaryComparatorFactory getBinaryComparatorFactory(Object leftType, Object rightType, boolean ascending,
            boolean ignoreCase) throws AlgebricksException;
}
