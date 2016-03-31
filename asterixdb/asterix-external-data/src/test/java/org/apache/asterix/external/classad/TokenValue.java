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
package org.apache.asterix.external.classad;

import org.apache.asterix.external.classad.Lexer.TokenType;
import org.apache.asterix.external.classad.Value.NumberFactor;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.commons.lang3.mutable.MutableBoolean;

public class TokenValue {
    private TokenType tt;
    private NumberFactor factor;
    private long intValue;
    private double realValue;
    private boolean boolValue;
    private AMutableCharArrayString strValue;
    private ClassAdTime time;

    public TokenValue() {
        tt = TokenType.LEX_TOKEN_ERROR;
        factor = NumberFactor.NO_FACTOR;
        intValue = 0;
        realValue = 0.0;
        boolValue = false;
        strValue = new AMutableCharArrayString();
        time = new ClassAdTime();
    }

    public void setTokenType(TokenType t) {
        tt = t;
    }

    public void setIntValue(long i, NumberFactor f) {
        intValue = i;
        factor = f;
    }

    public void setRealValue(double r, NumberFactor f) {
        realValue = r;
        factor = f;
    }

    public void setBoolValue(boolean b) {
        boolValue = b;
    }

    public void setStringValue(char[] str) {
        strValue.copyValue(str, str.length);
    }

    public void setStringValue(char[] str, int length) {
        strValue.copyValue(str, length);
    }

    public void setStringValue(AMutableCharArrayString aString) {
        strValue.setValue(aString);
    }

    public void setAbsTimeValue(ClassAdTime asecs) {
        time.setValue(asecs);
    }

    public void setRelTimeValue(double rsecs) {
        time.setValue((long) rsecs);
    }

    public TokenType getTokenType() {
        return tt;
    }

    public void getIntValue(AMutableInt64 i, AMutableNumberFactor f) {
        i.setValue(intValue);
        f.setFactor(factor);
    }

    public void getRealValue(AMutableDouble r, AMutableNumberFactor f) {
        r.setValue(realValue);
        f.setFactor(factor);
    }

    public void getBoolValue(MutableBoolean b) {
        b.setValue(boolValue);
    }

    void getStringValue(AMutableCharArrayString str) {
        str.copyValue(strValue.getValue(), strValue.getLength());
    }

    void getAbsTimeValue(ClassAdTime asecs) {
        asecs.setValue(time);
    }

    void getRelTimeValue(ClassAdTime rsecs) {
        rsecs.setValue(time.getRelativeTime());
    }

    void copyFrom(TokenValue tv) {
        tt = tv.tt;
        factor = tv.factor;
        intValue = tv.intValue;
        realValue = tv.realValue;
        boolValue = tv.boolValue;
        time.setValue(tv.time);
        strValue.setValue(tv.strValue);
    }

    public void reset() {
        tt = TokenType.LEX_TOKEN_ERROR;
        factor = NumberFactor.NO_FACTOR;
        intValue = 0;
        realValue = 0.0;
        boolValue = false;
        strValue.reset();
        time.reset();
    }

    public NumberFactor getFactor() {
        return factor;
    }

    public long getIntValue() {
        return intValue;
    }

    public double getRealValue() {
        return realValue;
    }

    public boolean getBoolValue() {
        return boolValue;
    }

    public AMutableCharArrayString getStrValue() {
        return strValue;
    }

    public ClassAdTime getTimeValue() {
        return time;
    }
}
