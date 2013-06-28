/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.om.pointables.visitor;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.om.pointables.AListPointable;
import edu.uci.ics.asterix.om.pointables.ARecordPointable;

/**
 * This interface is a visitor for all the three different IVisitablePointable
 * (Note that right now we have three pointable implementations for type
 * casting) implementations.
 */
public interface IVisitablePointableVisitor<R, T> {

    public R visit(AListPointable accessor, T arg) throws AsterixException;

    public R visit(ARecordPointable accessor, T arg) throws AsterixException;

    public R visit(AFlatValuePointable accessor, T arg) throws AsterixException;
}
