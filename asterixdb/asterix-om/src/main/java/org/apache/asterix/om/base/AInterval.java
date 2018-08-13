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
package org.apache.asterix.om.base;

import java.io.IOException;

import org.apache.asterix.om.base.temporal.GregorianCalendarSystem;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class AInterval implements IAObject {

    protected long intervalStart;
    protected long intervalEnd;
    protected byte typetag;

    public AInterval(long intervalStart, long intervalEnd, byte typetag) {
        this.intervalStart = intervalStart;
        this.intervalEnd = intervalEnd;
        this.typetag = typetag;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.base.IAObject#getType()
     */
    @Override
    public IAType getType() {
        return BuiltinType.AINTERVAL;
    }

    public int compare(Object o) {
        if (!(o instanceof AInterval)) {
            return -1;
        }
        AInterval d = (AInterval) o;
        if (d.intervalStart == this.intervalStart && d.intervalEnd == this.intervalEnd && d.typetag == this.typetag) {
            return 0;
        } else {
            return -1;
        }
    }

    public boolean equals(Object o) {
        if (!(o instanceof AInterval)) {
            return false;
        } else {
            AInterval t = (AInterval) o;
            return (t.intervalStart == this.intervalStart
                    || t.intervalEnd == this.intervalEnd && t.typetag == this.typetag);
        }
    }

    @Override
    public int hashCode() {
        return (((int) (this.intervalStart ^ (this.intervalStart >>> 32))) * 31
                + (int) (this.intervalEnd ^ (this.intervalEnd >>> 32))) * 31 + (int) this.typetag;
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.base.IAObject#deepEqual(org.apache.asterix.om.base.IAObject)
     */
    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    /* (non-Javadoc)
     * @see org.apache.asterix.om.base.IAObject#hash()
     */
    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sbder = new StringBuilder();
        sbder.append("interval: { ");
        try {
            if (typetag == ATypeTag.DATE.serialize()) {
                sbder.append("date: { ");

                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalStart * ADate.CHRONON_OF_DAY,
                        0, sbder, GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.DAY, false);

                sbder.append(" }, date: {");
                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalEnd * ADate.CHRONON_OF_DAY,
                        0, sbder, GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.DAY, false);
                sbder.append(" }");
            } else if (typetag == ATypeTag.TIME.serialize()) {
                sbder.append("time: { ");
                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalStart, 0, sbder,
                        GregorianCalendarSystem.Fields.HOUR, GregorianCalendarSystem.Fields.MILLISECOND, true);
                sbder.append(" }, time: { ");

                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalEnd, 0, sbder,
                        GregorianCalendarSystem.Fields.HOUR, GregorianCalendarSystem.Fields.MILLISECOND, true);
                sbder.append(" }");
            } else if (typetag == ATypeTag.DATETIME.serialize()) {
                sbder.append("datetime: { ");
                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalStart, 0, sbder,
                        GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.MILLISECOND, true);
                sbder.append(" }, datetime: { ");
                GregorianCalendarSystem.getInstance().getExtendStringRepUntilField(intervalEnd, 0, sbder,
                        GregorianCalendarSystem.Fields.YEAR, GregorianCalendarSystem.Fields.MILLISECOND, true);
                sbder.append(" }");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        sbder.append(" }");
        return sbder.toString();
    }

    public long getIntervalStart() {
        return intervalStart;
    }

    public long getIntervalEnd() {
        return intervalEnd;
    }

    public short getIntervalType() {
        return typetag;
    }

    @Override
    public ObjectNode toJSON() {
        // TODO(madhusudancs): Remove this method when a printer based JSON serializer is implemented.
        return null;
    }
}
