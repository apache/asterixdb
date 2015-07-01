/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.library;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.IAObject;

public interface IResultCollector {

    public void writeIntResult(int result) throws AsterixException;

    public void writeFloatResult(float result) throws AsterixException;

    public void writeDoubleResult(double result) throws AsterixException;

    public void writeStringResult(String result) throws AsterixException;

    public void writeRecordResult(ARecord result) throws AsterixException;

    public void writeListResult(AOrderedList list) throws AsterixException;

    public IAObject getComplexTypeResultHolder();

    public DataOutput getDataOutput();
}
