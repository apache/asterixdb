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

import java.util.Calendar;
import java.util.TimeZone;

import org.apache.asterix.external.classad.Value.NumberFactor;
import org.apache.asterix.external.classad.Value.ValueType;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class Literal extends ExprTree {
    /**
     * Represents the literals of the ClassAd language, such as integers,
     * reals, booleans, strings, undefined and real.
     */
    // literal specific information
    private final Value value;
    private NumberFactor factor;

    public Literal(ClassAdObjectPool objectPool) {
        super(objectPool);
        factor = Value.NumberFactor.NO_FACTOR;
        value = new Value(objectPool);
    }

    @Override
    public String toString() {
        switch (value.getValueType()) {
            case ABSOLUTE_TIME_VALUE:
                return "datetime(" + value + ")";
            case BOOLEAN_VALUE:
                return String.valueOf(value.getBoolVal());
            case CLASSAD_VALUE:
            case LIST_VALUE:
            case SLIST_VALUE:
            case INTEGER_VALUE:
            case NULL_VALUE:
            case REAL_VALUE:
                return value.toString();
            case ERROR_VALUE:
                return "\"error\"";
            case RELATIVE_TIME_VALUE:
                return "duration(" + value + ")";
            case UNDEFINED_VALUE:
                return "\"undefined\"";
            case STRING_VALUE:
                return "\"" + value.toString() + "\"";
            default:
                return null;
        }
    }

    @Override
    public ExprTree copy() throws HyracksDataException {
        Literal newTree = objectPool.literalPool.get();
        newTree.copyFrom(this);
        return newTree;
    }

    public void copyFrom(Literal literal) throws HyracksDataException {
        super.copyFrom(literal);
        value.setValue(literal.value);
        factor = literal.factor;
        return;
    }

    public static Literal createReal(AMutableCharArrayString aString, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        double real;
        real = Double.parseDouble(aString.toString());
        val.setRealValue(real);
        return createLiteral(val, objectPool);
    }

    public static Literal createReal(String aString, ClassAdObjectPool objectPool) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        double real;
        real = Double.parseDouble(aString.toString());
        val.setRealValue(real);
        return createLiteral(val, objectPool);
    }

    public static Literal createAbsTime(ClassAdTime tim, ClassAdObjectPool objectPool) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        if (tim == null) { // => current time/offset
            tim = objectPool.classAdTimePool.get();
        }
        val.setAbsoluteTimeValue(tim);
        return (createLiteral(val, objectPool));
    }

    /* Creates an absolute time literal, from the string timestr,
     *parsing it as the regular expression:
     D* dddd [D* dd [D* dd [D* dd [D* dd [D* dd D*]]]]] [-dd:dd | +dd:dd | z | Z]
     D => non-digit, d=> digit
     Ex - 2003-01-25T09:00:00-06:00
    */
    public static Literal createAbsTime(AMutableCharArrayString timeStr, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        boolean offset = false; // to check if the argument conatins a timezone offset parameter

        AMutableInt32 tzhr = objectPool.int32Pool.get(); // corresponds to 1st "dd" in -|+dd:dd
        AMutableInt32 tzmin = objectPool.int32Pool.get(); // corresponds to 2nd "dd" in -|+dd:dd

        int len = timeStr.getLength();
        AMutableInt32 index = objectPool.int32Pool.get();
        index.setValue(len - 1);
        prevNonSpaceChar(timeStr, index);
        AMutableInt32 i = objectPool.int32Pool.get();
        i.setValue(index.getIntegerValue());

        if ((timeStr.charAt(i.getIntegerValue()) == 'z') || (timeStr.charAt(i.getIntegerValue()) == 'Z')) { // z|Z corresponds to a timezone offset of 0
            offset = true;
            timeStr.erase(i.getIntegerValue()); // remove the offset section from the string
        } else if (timeStr.charAt(len - 5) == '+' || timeStr.charAt(len - 5) == '-') {
            offset = extractTimeZone(timeStr, tzhr, tzmin);
        } else if ((timeStr.charAt(len - 6) == '+' || timeStr.charAt(len - 6) == '-')
                && timeStr.charAt(len - 3) == ':') {
            timeStr.erase(len - 3, 1);
            offset = extractTimeZone(timeStr, tzhr, tzmin);
        }

        i.setValue(0);
        len = timeStr.getLength();
        nextDigitChar(timeStr, i);
        if (i.getIntegerValue() > len - 4) { // string has to contain dddd (year)
            val.setErrorValue();
            return (createLiteral(val, objectPool));
        }
        int tm_year, tm_mon = 0, tm_mday = 0, tm_hour = 0, tm_min = 0, tm_sec = 0;
        tm_year = Integer.parseInt((timeStr.substr(i.getIntegerValue(), 4)));// - 1900;
        i.setValue(i.getIntegerValue() + 4);
        nextDigitChar(timeStr, i);
        if (i.getIntegerValue() <= len - 2) {
            tm_mon = Integer.parseInt(timeStr.substr(i.getIntegerValue(), 2)) - 1;
            i.setValue(i.getIntegerValue() + 2);
        }
        nextDigitChar(timeStr, i);

        if (i.getIntegerValue() <= len - 2) {
            tm_mday = Integer.parseInt(timeStr.substr(i.getIntegerValue(), 2));
            i.setValue(i.getIntegerValue() + 2);
        }
        nextDigitChar(timeStr, i);

        if (i.getIntegerValue() <= len - 2) {
            tm_hour = Integer.parseInt(timeStr.substr(i.getIntegerValue(), 2));
            i.setValue(i.getIntegerValue() + 2);
        }
        nextDigitChar(timeStr, i);

        if (i.getIntegerValue() <= len - 2) {
            tm_min = Integer.parseInt(timeStr.substr(i.getIntegerValue(), 2));
            i.setValue(i.getIntegerValue() + 2);
        }
        nextDigitChar(timeStr, i);

        if (i.getIntegerValue() <= len - 2) {
            tm_sec = Integer.parseInt(timeStr.substr(i.getIntegerValue(), 2));
            i.setValue(i.getIntegerValue() + 2);
        }
        nextDigitChar(timeStr, i);

        if ((i.getIntegerValue() <= len - 1) && (Character.isDigit(timeStr.charAt(i.getIntegerValue())))) { // there should be no more digit characters once the required
            val.setErrorValue(); // parameteres are parsed
            return (createLiteral(val, objectPool));
        }
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        cal.clear();
        cal.set(tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec);
        ClassAdTime time = objectPool.classAdTimePool.get();
        time.setValue(cal.getTimeInMillis());
        if (offset) {
            time.setTimeZone((tzhr.getIntegerValue() * 3600000) + (tzmin.getIntegerValue() * 60000));
        } else {
            // if offset is not specified, the offset of the current locality is taken
            time.setDefaultTimeZone();
        }
        val.setAbsoluteTimeValue(time);
        return (createLiteral(val, objectPool));
    }

    public Literal createRelTime(ClassAdTime t1, ClassAdTime t2) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        val.setRelativeTimeValue(t1.subtract(t2));
        return (createLiteral(val, objectPool));
    }

    Literal createRelTime(ClassAdTime secs) throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        val.setRelativeTimeValue(secs);
        return (createLiteral(val, objectPool));
    }

    /* Creates a relative time literal, from the string timestr,
     *parsing it as [[[days+]hh:]mm:]ss
     * Ex - 1+00:02:00
     */
    public static Literal createRelTime(AMutableCharArrayString timeStr, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        Value val = objectPool.valuePool.get();
        ClassAdTime rsecs = objectPool.classAdTimePool.get();

        int len = timeStr.getLength();
        double secs = 0;
        int mins = 0;
        int hrs = 0;
        int days = 0;
        boolean negative = false;
        AMutableInt32 i = objectPool.int32Pool.get();
        i.setValue(len - 1);
        prevNonSpaceChar(timeStr, i);
        // checking for 'sec' parameter & collecting it if present (ss.sss)
        if ((i.getIntegerValue() >= 0)
                && ((timeStr.charAt(i.getIntegerValue()) == 's') || (timeStr.charAt(i.getIntegerValue()) == 'S')
                        || (Character.isDigit(timeStr.charAt(i.getIntegerValue()))))) {
            if ((timeStr.charAt(i.getIntegerValue()) == 's') || (timeStr.charAt(i.getIntegerValue()) == 'S')) {
                i.setValue(i.getIntegerValue() - 1);
            }
            prevNonSpaceChar(timeStr, i);
            AMutableCharArrayString revSecStr = objectPool.strPool.get();
            while ((i.getIntegerValue() >= 0) && (Character.isDigit(timeStr.charAt(i.getIntegerValue())))) {
                revSecStr.appendChar(timeStr.charAt(i.getIntegerValue()));
                i.setValue(i.getIntegerValue() - 1);
            }
            if ((i.getIntegerValue() >= 0) && (timeStr.charAt(i.getIntegerValue()) == '.')) {
                revSecStr.appendChar(timeStr.charAt(i.getIntegerValue()));
                i.setValue(i.getIntegerValue() - 1);
                while ((i.getIntegerValue() >= 0) && (Character.isDigit(timeStr.charAt(i.getIntegerValue())))) {
                    revSecStr.appendChar(timeStr.charAt(i.getIntegerValue()));
                    i.setValue(i.getIntegerValue() - 1);
                }
            }
            secs = revDouble(revSecStr, objectPool);
        }

        prevNonSpaceChar(timeStr, i);
        // checking for 'min' parameter
        if ((i.getIntegerValue() >= 0) && ((timeStr.charAt(i.getIntegerValue()) == 'm')
                || (timeStr.charAt(i.getIntegerValue()) == 'M') || (timeStr.charAt(i.getIntegerValue()) == ':'))) {
            i.setValue(i.getIntegerValue() - 1);
            AMutableCharArrayString revMinStr = objectPool.strPool.get();
            prevNonSpaceChar(timeStr, i);
            while ((i.getIntegerValue() >= 0) && (Character.isDigit(timeStr.charAt(i.getIntegerValue())))) {
                revMinStr.appendChar(timeStr.charAt(i.getIntegerValue()));
                i.setValue(i.getIntegerValue() - 1);
            }
            mins = revInt(revMinStr, objectPool);
        }

        prevNonSpaceChar(timeStr, i);
        // checking for 'hrs' parameter
        if ((i.getIntegerValue() >= 0) && ((timeStr.charAt(i.getIntegerValue()) == 'h')
                || (timeStr.charAt(i.getIntegerValue()) == 'H') || (timeStr.charAt(i.getIntegerValue()) == ':'))) {
            i.setValue(i.getIntegerValue() - 1);
            AMutableCharArrayString revHrStr = objectPool.strPool.get();
            prevNonSpaceChar(timeStr, i);
            while ((i.getIntegerValue() >= 0) && (Character.isDigit(timeStr.charAt(i.getIntegerValue())))) {
                revHrStr.appendChar(timeStr.charAt(i.getIntegerValue()));
                i.setValue(i.getIntegerValue() - 1);
            }
            hrs = revInt(revHrStr, objectPool);
        }

        prevNonSpaceChar(timeStr, i);
        // checking for 'days' parameter
        if ((i.getIntegerValue() >= 0) && ((timeStr.charAt(i.getIntegerValue()) == 'd')
                || (timeStr.charAt(i.getIntegerValue()) == 'D') || (timeStr.charAt(i.getIntegerValue()) == '+'))) {
            i.setValue(i.getIntegerValue() - 1);
            AMutableCharArrayString revDayStr = objectPool.strPool.get();
            prevNonSpaceChar(timeStr, i);
            while ((i.getIntegerValue() >= 0) && (Character.isDigit(timeStr.charAt(i.getIntegerValue())))) {
                revDayStr.appendChar(timeStr.charAt(i.getIntegerValue()));
                i.setValue(i.getIntegerValue() - 1);
            }
            days = revInt(revDayStr, objectPool);
        }

        prevNonSpaceChar(timeStr, i);
        // checking for '-' operator
        if ((i.getIntegerValue() >= 0) && (timeStr.charAt(i.getIntegerValue()) == '-')) {
            negative = true;
            i.setValue(i.getIntegerValue() - 1);
        }

        prevNonSpaceChar(timeStr, i);

        if ((i.getIntegerValue() >= 0) && (!(Character.isWhitespace(timeStr.charAt(i.getIntegerValue()))))) { // should not conatin any non-space char beyond -,d,h,m,s
            val.setErrorValue();
            return (createLiteral(val, objectPool));
        }

        rsecs.setRelativeTime(
                (long) ((negative ? -1 : +1) * (days * 86400000 + hrs * 3600000 + mins * 60000 + secs * 1000.0)));
        val.setRelativeTimeValue(rsecs);

        return (createLiteral(val, objectPool));
    }

    /* Function which iterates through the string Str from the location 'index',
     *returning the index of the next digit-char
     */
    public static void nextDigitChar(AMutableCharArrayString Str, AMutableInt32 index) {
        int len = Str.getLength();
        int i = index.getIntegerValue();
        while ((i < len) && (!Character.isDigit(Str.charAt(i)))) {
            i++;
        }
        index.setValue(i);
    }

    /* Function which iterates through the string Str backwards from the location 'index'
     *returning the index of the first occuring non-space character
     */
    public static void prevNonSpaceChar(AMutableCharArrayString Str, AMutableInt32 index) {
        int i = index.getIntegerValue();
        while ((i >= 0) && (Character.isWhitespace(Str.charAt(i)))) {
            i--;
        }
        index.setValue(i);
    }

    /* Function which takes a number in string format, and reverses the
     * order of the digits & returns the corresponding number as an
     * integer.
     */
    public static int revInt(AMutableCharArrayString revNumStr, ClassAdObjectPool objectPool) {
        AMutableCharArrayString numStr = objectPool.strPool.get();
        for (int i = revNumStr.getLength() - 1; i >= 0; i--) {
            numStr.appendChar(revNumStr.charAt(i));
        }
        return Integer.parseInt(numStr.toString());
    }

    /* Function which takes a number in string format, and reverses the
     * order of the digits & returns the corresponding number as a double.
     */
    public static double revDouble(AMutableCharArrayString revNumStr, ClassAdObjectPool objectPool) {
        AMutableCharArrayString numStr = objectPool.strPool.get();
        for (int i = revNumStr.getLength() - 1; i >= 0; i--) {
            numStr.appendChar(revNumStr.charAt(i));
        }
        return Double.parseDouble(numStr.toString());
    }

    /* function which returns the timezone offset corresponding to the argument epochsecs,
     *  which is the number of seconds since the epoch
     */
    public static int findOffset(ClassAdTime epochsecs) {
        return Util.timezoneOffset(epochsecs);
    }

    public static Literal createLiteral(Value val, NumberFactor f, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        if (val.getType() == ValueType.CLASSAD_VALUE || val.getType() == ValueType.LIST_VALUE
                || val.getType() == ValueType.SLIST_VALUE) {
            throw new HyracksDataException("list and classad values are not literals");
        }
        Literal lit = objectPool.literalPool.get();
        lit.value.setValue(val);
        if (!val.isIntegerValue() && !val.isRealValue()) {
            f = NumberFactor.NO_FACTOR;
        }
        lit.factor = f;
        return lit;
    }

    public static void createLiteral(Literal lit, Value val, NumberFactor f) throws HyracksDataException {
        if (val.getType() == ValueType.CLASSAD_VALUE || val.getType() == ValueType.LIST_VALUE
                || val.getType() == ValueType.SLIST_VALUE) {
            throw new HyracksDataException("list and classad values are not literals");
        }
        lit.value.setValue(val);
        if (!val.isIntegerValue() && !val.isRealValue()) {
            f = NumberFactor.NO_FACTOR;
        }
        lit.factor = f;
    }

    public static Literal createLiteral(Value val, ClassAdObjectPool objectPool) throws HyracksDataException {
        return createLiteral(val, NumberFactor.NO_FACTOR, objectPool);
    }

    public void GetValue(Value val) throws HyracksDataException {
        AMutableInt64 i = objectPool.int64Pool.get();
        AMutableDouble r = objectPool.doublePool.get();
        val.setValue(value);

        // if integer or real, multiply by the factor
        if (val.isIntegerValue(i)) {
            if (factor != NumberFactor.NO_FACTOR) {
                val.setRealValue((i.getLongValue()) * Value.ScaleFactor[factor.ordinal()]);
            }
        } else if (val.isRealValue(r)) {
            if (factor != NumberFactor.NO_FACTOR) {
                val.setRealValue(r.getDoubleValue() * Value.ScaleFactor[factor.ordinal()]);
            }
        }
    }

    public void getComponents(Value val, AMutableNumberFactor factor) throws HyracksDataException {
        val.setValue(value);
        factor.setFactor(this.factor);
    }

    @Override
    public boolean sameAs(ExprTree tree) {
        boolean is_same;
        ExprTree pSelfTree = tree.self();

        if (this == pSelfTree) {
            is_same = true;
        } else if (pSelfTree.getKind() != NodeKind.LITERAL_NODE) {
            is_same = false;
        } else {
            Literal other_literal = (Literal) pSelfTree;
            is_same = (factor == other_literal.factor && value.sameAs(other_literal.value));
        }
        return is_same;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Literal) {
            Literal literal = (Literal) o;
            return sameAs(literal);
        }
        return false;
    }

    @Override
    public boolean privateEvaluate(EvalState eval, Value val) throws HyracksDataException {
        AMutableInt64 i = objectPool.int64Pool.get();
        AMutableDouble r = objectPool.doublePool.get();

        val.setValue(value);

        // if integer or real, multiply by the factor
        if (val.isIntegerValue(i)) {
            if (factor != NumberFactor.NO_FACTOR) {
                val.setRealValue((i.getLongValue()) * Value.ScaleFactor[factor.ordinal()]);
            } else {
                val.setIntegerValue(i.getLongValue());
            }
        } else if (val.isRealValue(r)) {
            val.setRealValue(r.getDoubleValue() * Value.ScaleFactor[factor.ordinal()]);
        }
        return true;
    }

    @Override
    public boolean privateEvaluate(EvalState state, Value val, ExprTreeHolder tree) throws HyracksDataException {
        privateEvaluate(state, val);
        tree.setInnerTree(copy());
        return (tree != null);
    }

    @Override
    public boolean privateFlatten(EvalState state, Value val, ExprTreeHolder tree, AMutableInt32 i)
            throws HyracksDataException {
        tree.reset();
        return privateEvaluate(state, val);
    }

    public static boolean extractTimeZone(AMutableCharArrayString timeStr, AMutableInt32 tzhr, AMutableInt32 tzmin) {
        int len = timeStr.getLength();
        int i = len - 1;
        boolean offset = false;
        String offStr = timeStr.toString().substring(i - 4, len);

        if (((offStr.charAt(0) == '+') || (offStr.charAt(0) == '-')) && (Character.isDigit(offStr.charAt(1)))
                && (Character.isDigit(offStr.charAt(2))) && (Character.isDigit(offStr.charAt(3)))
                && (Character.isDigit(offStr.charAt(4)))) {
            offset = true;
            timeStr.erase(i - 4, 5);
            if (offStr.charAt(0) == '+') {
                tzhr.setValue(Integer.parseInt(offStr.substring(1, 3)));
                tzmin.setValue(Integer.parseInt(offStr.substring(3, 5)));
            } else {
                tzhr.setValue((-1) * Integer.parseInt(offStr.substring(1, 3)));
                tzmin.setValue((-1) * Integer.parseInt(offStr.substring(3, 5)));
            }
        }
        return offset;
    }

    @Override
    public NodeKind getKind() {
        return NodeKind.LITERAL_NODE;
    }

    @Override
    protected void privateSetParentScope(ClassAd scope) {
    }

    @Override
    public void reset() {
        value.reset();
        factor = NumberFactor.NO_FACTOR;
    }

    public Value getValue() {
        return value;
    }
}
