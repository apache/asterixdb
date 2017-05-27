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

package org.apache.asterix.om.pointables.visitor;

import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This interface is a visitor for all the three different IVisitablePointable
 * (Note that right now we have three pointable implementations for type
 * casting) implementations.
 */
public interface IVisitablePointableVisitor<R, T> {

    R visit(AListVisitablePointable accessor, T arg) throws HyracksDataException;

    R visit(ARecordVisitablePointable accessor, T arg) throws HyracksDataException;

    R visit(AFlatValuePointable accessor, T arg) throws HyracksDataException;
}
