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
package org.apache.asterix.om.lazy;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * A visitor for ADM values which utilizes the lazy visitable:
 *
 * @param <R> return type
 * @param <T> argument type
 * @see AbstractLazyVisitablePointable
 * @see AbstractLazyNestedVisitablePointable
 */
public interface ILazyVisitablePointableVisitor<R, T> {
    /**
     * Visit record value
     *
     * @param pointable either typed {@link TypedRecordLazyVisitablePointable} or
     *                  untyped {@link RecordLazyVisitablePointable}
     * @param arg       visitor argument
     * @return return value
     */
    R visit(RecordLazyVisitablePointable pointable, T arg) throws HyracksDataException;

    /**
     * Visit list value
     *
     * @param pointable either a list with fixed-length items {@link FixedListLazyVisitablePointable} or
     *                  a list with variable-length items {@link VariableListLazyVisitablePointable}
     * @param arg       visitor argument
     * @return return value
     */

    R visit(AbstractListLazyVisitablePointable pointable, T arg) throws HyracksDataException;

    /**
     * Atomic values
     *
     * @param pointable any flat item (e.g., {@link org.apache.asterix.om.types.ATypeTag#BIGINT}
     * @param arg       visitor argument
     * @return return value
     * @throws HyracksDataException
     */
    R visit(FlatLazyVisitablePointable pointable, T arg) throws HyracksDataException;
}
