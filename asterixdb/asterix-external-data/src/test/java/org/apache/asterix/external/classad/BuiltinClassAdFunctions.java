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

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.external.classad.ExprTree.NodeKind;
import org.apache.asterix.external.classad.Value.ValueType;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.external.library.ClassAdParser;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class BuiltinClassAdFunctions {

    public static final ClassAdFunc IsType = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            // need a single argument
            if (argList.size() != 1) {
                val.setErrorValue();
                return true;
            }
            // Evaluate the argument
            if (!argList.getExprList().get(0).publicEvaluate(state, val)) {
                val.setErrorValue();
                return false;
            }
            // check if the value was of the required type
            switch (name.toLowerCase()) {
                case "isundefined":
                    val.setBooleanValue(val.isUndefinedValue());
                    break;
                case "iserror":
                    val.setBooleanValue(val.isErrorValue());
                    break;
                case "isinteger":
                    val.setBooleanValue(val.isIntegerValue());
                    break;
                case "isstring":
                    val.setBooleanValue(val.isStringValue());
                    break;
                case "isreal":
                    val.setBooleanValue(val.isRealValue());
                    break;
                case "isboolean":
                    val.setBooleanValue(val.isBooleanValue());
                    break;
                case "isclassad":
                    val.setBooleanValue(val.isClassAdValue());
                    break;
                case "islist":
                    val.setBooleanValue(val.isListValue());
                    break;
                case "isabstime":
                    val.setBooleanValue(val.isAbsoluteTimeValue());
                    break;
                case "isreltime":
                    val.setBooleanValue(val.isRelativeTimeValue());
                    break;
                default:
                    val.setErrorValue();
            }
            return (true);
        }
    };
    public static final ClassAdFunc TestMember = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg0 = objectPool.valuePool.get();
            Value arg1 = objectPool.valuePool.get();
            Value cArg = objectPool.valuePool.get();

            ExprList el = objectPool.exprListPool.get();
            MutableBoolean b = objectPool.boolPool.get();
            boolean useIS = name.equalsIgnoreCase("identicalmember");

            // need two arguments
            if (argList.size() != 2) {
                val.setErrorValue();
                return (true);
            }

            // Evaluate the arg list
            if (!argList.get(0).publicEvaluate(state, arg0) || !argList.get(1).publicEvaluate(state, arg1)) {
                val.setErrorValue();
                return false;
            }

            // if the second arg (a list) is undefined, or the first arg is
            // undefined and we're supposed to test for strict comparison, the
            // result is 'undefined'
            if (arg1.isUndefinedValue() || (!useIS && arg0.isUndefinedValue())) {
                val.setUndefinedValue();
                return true;
            }

            // Swap
            if (arg0.isListValue() && !arg1.isListValue()) {
                Value swap = objectPool.valuePool.get();
                swap.setValue(arg0);
                arg0.setValue(arg1);
                arg1.setValue(swap);
            }

            // arg1 must be a list; arg0 must be comparable
            if (!arg1.isListValue() || arg0.isListValue() || arg0.isClassAdValue()) {
                val.setErrorValue();
                return true;
            }

            // if we're using strict comparison, arg0 can't be 'error'
            if (!useIS && arg0.isErrorValue()) {
                val.setErrorValue();
                return (true);
            }

            // check for membership
            arg1.isListValue(el);
            for (ExprTree tree : el.getExprList()) {
                if (!tree.publicEvaluate(state, cArg)) {
                    val.setErrorValue();
                    return (false);
                }
                Operation.operate(useIS ? Operation.OpKind_IS_OP : Operation.OpKind_EQUAL_OP, cArg, arg0, val,
                        objectPool);
                if (val.isBooleanValue(b) && b.booleanValue()) {
                    return true;
                }
            }
            val.setBooleanValue(false);
            return true;
        }
    };
    public static final ClassAdFunc Size = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            ExprList listToSize = objectPool.exprListPool.get();
            ClassAd classadToSize = objectPool.classAdPool.get();
            AMutableInt32 length = objectPool.int32Pool.get();
            length.setValue(0);
            // we accept only one argument
            if (argList.size() != 1) {
                val.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                val.setErrorValue();
                return false;
            } else if (arg.isUndefinedValue()) {
                val.setUndefinedValue();
                return true;
            } else if (arg.isListValue(listToSize)) {
                val.setIntegerValue(listToSize.size());
                return true;
            } else if (arg.isClassAdValue(classadToSize)) {
                val.setIntegerValue(classadToSize.size());
                return true;
            } else if (arg.isStringValue(length)) {
                val.setIntegerValue(length.getIntegerValue());
                return true;
            } else {
                val.setErrorValue();
                return true;
            }
        }
    };
    public static final ClassAdFunc SumAvg = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {

            Value listElementValue = objectPool.valuePool.get();
            Value listVal = objectPool.valuePool.get();
            Value numElements = objectPool.valuePool.get();
            Value result = objectPool.valuePool.get();
            ExprList listToSum = objectPool.exprListPool.get();
            MutableBoolean first = objectPool.boolPool.get();
            AMutableInt64 len = objectPool.int64Pool.get();
            len.setValue(0);
            boolean onlySum = name.equalsIgnoreCase("sum");

            // we accept only one argument
            if (argList.size() != 1) {
                val.setErrorValue();
                return (true);
            }

            // argument must Evaluate to a list
            if (!argList.get(0).publicEvaluate(state, listVal)) {
                val.setErrorValue();
                return false;
            } else if (listVal.isUndefinedValue()) {
                val.setUndefinedValue();
                return true;
            } else if (!listVal.isListValue(listToSum)) {
                val.setErrorValue();
                return (true);
            }

            result.setUndefinedValue();
            len.setValue(0);
            first.setValue(true);

            // Walk over each element in the list, and sum.
            for (ExprTree listElement : listToSum.getExprList()) {
                len.setValue(len.getLongValue() + 1);
                // Make sure this element is a number.
                if (!listElement.publicEvaluate(state, listElementValue)) {
                    val.setErrorValue();
                    return false;
                } else if (!listElementValue.isRealValue() && !listElementValue.isIntegerValue()) {
                    val.setErrorValue();
                    return true;
                }

                // Either take the number if it's the first,
                // or add to the running sum.
                if (first.booleanValue()) {
                    result.setValue(listElementValue);
                    first.setValue(false);
                } else {
                    Operation.operate(Operation.OpKind_ADDITION_OP, result, listElementValue, result, objectPool);
                }

            }

            // if the sum() function was called, we don't need to find the average
            if (onlySum) {
                val.setValue(result);
                return true;
            }

            if (len.getLongValue() > 0) {
                numElements.setRealValue(len.getLongValue());
                Operation.operate(Operation.OpKind_DIVISION_OP, result, numElements, result, objectPool);
            } else {
                val.setUndefinedValue();
            }

            val.setValue(result);
            return true;
        }

    };
    public static final ClassAdFunc MinMax = new ClassAdFunc() {

        @Override
        public boolean call(String fn, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value listElementValue = objectPool.valuePool.get();
            Value listVal = objectPool.valuePool.get();
            Value cmp = objectPool.valuePool.get();
            Value result = objectPool.valuePool.get();
            ExprList listToBound = objectPool.exprListPool.get();
            boolean first = true;
            MutableBoolean b = objectPool.boolPool.get();
            b.setValue(false);
            int comparisonOperator;

            // we accept only one argument
            if (argList.size() != 1) {
                val.setErrorValue();
                return true;
            }

            // first argument must Evaluate to a list
            if (!argList.get(0).publicEvaluate(state, listVal)) {
                val.setErrorValue();
                return false;
            } else if (listVal.isUndefinedValue()) {
                val.setUndefinedValue();
                return true;
            } else if (!listVal.isListValue(listToBound)) {
                val.setErrorValue();
                return true;
            }

            // fn is either "min..." or "max..."
            if (Character.toLowerCase(fn.charAt(1)) == 'i') {
                comparisonOperator = Operation.OpKind_LESS_THAN_OP;
            } else {
                comparisonOperator = Operation.OpKind_GREATER_THAN_OP;
            }

            result.setUndefinedValue();

            // Walk over the list, calculating the bound the whole way.
            for (ExprTree listElement : listToBound.getExprList()) {
                // For this element of the list, make sure it is
                // acceptable.
                if (!listElement.publicEvaluate(state, listElementValue)) {
                    val.setErrorValue();
                    return false;
                } else if (!listElementValue.isRealValue() && !listElementValue.isIntegerValue()) {
                    val.setErrorValue();
                    return true;
                }

                // If it's the first element, copy it to the bound,
                // otherwise compare to decide what to do.
                if (first) {
                    result.setValue(listElementValue);
                    first = false;
                } else {
                    Operation.operate(comparisonOperator, listElementValue, result, cmp, objectPool);
                    if (cmp.isBooleanValue(b) && b.booleanValue()) {
                        result.setValue(listElementValue);
                    }
                }
            }

            val.setValue(result);
            return true;
        }

    };
    public static final ClassAdFunc ListCompare = new ClassAdFunc() {

        @Override
        public boolean call(String fn, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {

            Value listElementValue = objectPool.valuePool.get();
            Value listVal = objectPool.valuePool.get();
            Value compareVal = objectPool.valuePool.get();
            Value stringValue = objectPool.valuePool.get();
            ExprList listToCompare = objectPool.exprListPool.get();
            boolean needAllMatch;
            AMutableCharArrayString comparison_string = objectPool.strPool.get();
            int comparisonOperator;

            // We take three arguments:
            // The operator to use, as a string.
            // The list
            // The thing we are comparing against.
            if (argList.size() != 3) {
                val.setErrorValue();
                return true;
            }

            // The first argument must be a string
            if (!argList.get(0).publicEvaluate(state, stringValue)) {
                val.setErrorValue();
                return false;
            } else if (stringValue.isUndefinedValue()) {
                val.setUndefinedValue();
                return true;
            } else if (!stringValue.isStringValue(comparison_string)) {
                val.setErrorValue();
                return true;
            }

            // Decide which comparison to do, or give an error
            switch (comparison_string.toString()) {
                case "<":
                    comparisonOperator = Operation.OpKind_LESS_THAN_OP;
                    break;
                case "<=":
                    comparisonOperator = Operation.OpKind_LESS_OR_EQUAL_OP;
                    break;
                case "!=":
                    comparisonOperator = Operation.OpKind_NOT_EQUAL_OP;
                    break;
                case "==":
                    comparisonOperator = Operation.OpKind_EQUAL_OP;
                    break;
                case ">":
                    comparisonOperator = Operation.OpKind_GREATER_THAN_OP;
                    break;
                case ">=":
                    comparisonOperator = Operation.OpKind_GREATER_OR_EQUAL_OP;
                    break;
                case "is":
                    comparisonOperator = Operation.OpKind_META_EQUAL_OP;
                    break;
                case "isnt":
                    comparisonOperator = Operation.OpKind_META_NOT_EQUAL_OP;
                    break;
                default:
                    val.setErrorValue();
                    return true;
            }

            // The second argument must Evaluate to a list
            if (!argList.get(1).publicEvaluate(state, listVal)) {
                val.setErrorValue();
                return false;
            } else if (listVal.isUndefinedValue()) {
                val.setUndefinedValue();
                return true;
            } else if (!listVal.isListValue(listToCompare)) {
                val.setErrorValue();
                return true;
            }

            // The third argument is something to compare against.
            if (!argList.get(2).publicEvaluate(state, compareVal)) {
                val.setErrorValue();
                return false;
            } else if (listVal.isUndefinedValue()) {
                val.setUndefinedValue();
                return true;
            }

            // Finally, we decide what to do exactly, based on our name.
            if (fn.equalsIgnoreCase("anycompare")) {
                needAllMatch = false;
                val.setBooleanValue(false);
            } else {
                needAllMatch = true;
                val.setBooleanValue(true);
            }

            // Walk over the list
            for (ExprTree listElement : listToCompare.getExprList()) {
                // For this element of the list, make sure it is
                // acceptable.
                if (!listElement.publicEvaluate(state, listElementValue)) {
                    val.setErrorValue();
                    return false;
                } else {
                    Value compareResult = objectPool.valuePool.get();
                    MutableBoolean b = objectPool.boolPool.get();

                    Operation.operate(comparisonOperator, listElementValue, compareVal, compareResult, objectPool);
                    if (!compareResult.isBooleanValue(b)) {
                        if (compareResult.isUndefinedValue()) {
                            if (needAllMatch) {
                                val.setBooleanValue(false);
                                return true;
                            }
                        } else {
                            val.setErrorValue();
                            return true;
                        }
                        return true;
                    } else if (b.booleanValue()) {
                        if (!needAllMatch) {
                            val.setBooleanValue(true);
                            return true;
                        }
                    } else {
                        if (needAllMatch) {
                            // we failed, because it didn't match
                            val.setBooleanValue(false);
                            return true;
                        }
                    }
                }
            }

            if (needAllMatch) {
                // They must have all matched, because nothing failed,
                // which would have returned.
                val.setBooleanValue(true);
            } else {
                // Nothing must have matched, since we would have already
                // returned.
                val.setBooleanValue(false);
            }

            return true;
        }

    };
    public static final ClassAdFunc timeZoneOffset = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            // no arguments
            if (argList.size() > 0) {
                val.setErrorValue();
                return (true);
            }
            val.setRelativeTimeValue(objectPool.classAdTimePool.get());
            return (true);
        }
    };
    public static final ClassAdFunc debug = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            return false;
        }
    };
    public static final ClassAdFunc formatTime = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value time_arg = objectPool.valuePool.get();
            Value format_arg = objectPool.valuePool.get();
            AMutableInt64 int64 = objectPool.int64Pool.get();
            int64.setValue(0);
            ClassAdTime epoch_time = objectPool.classAdTimePool.get();
            ClassAdTime time_components = objectPool.classAdTimePool.get();
            time_components.setTimeZone("GMT");
            ClassAd splitClassAd = objectPool.classAdPool.get();
            String format;
            int number_of_args;
            boolean did_eval;
            did_eval = true;
            number_of_args = argList.size();
            if (number_of_args == 0) {
                epoch_time.setEpochTime();
                Util.getLocalTime(epoch_time, time_components);
                format = "%c";
                make_formatted_time(time_components, format, result);
            } else if (number_of_args < 3) {
                // The first argument should be our time and should
                // not be a relative time.
                if (!argList.get(0).publicEvaluate(state, time_arg)) {
                    did_eval = false;
                } else if (time_arg.isRelativeTimeValue()) {
                    result.setErrorValue();
                } else if (time_arg.isAbsoluteTimeValue(time_components)) {
                } else if (!time_arg.isClassAdValue(splitClassAd) /* doSplitTime(time_arg, splitClassAd) */) {
                    result.setErrorValue();
                } else {
                    if (!splitClassAd.evaluateAttrInt("Seconds", int64)) {
                        time_components.setSeconds(0);
                    } else {
                        time_components.setSeconds((int) int64.getLongValue());
                    }
                    if (!splitClassAd.evaluateAttrInt("Minutes", int64)) {
                        time_components.setMinutes(0);
                    } else {
                        time_components.setMinutes((int) int64.getLongValue());
                    }
                    if (!splitClassAd.evaluateAttrInt("Hours", int64)) {
                        time_components.setHours(0);
                    } else {
                        time_components.setHours((int) int64.getLongValue());
                    }
                    if (!splitClassAd.evaluateAttrInt("Day", int64)) {
                        time_components.setDayOfMonth(0);
                    } else {
                        time_components.setDayOfMonth((int) int64.getLongValue());
                    }
                    if (!splitClassAd.evaluateAttrInt("Month", int64)) {
                        time_components.setMonth(0);
                    } else {
                        time_components.setMonth((int) int64.getLongValue() - 1);
                    }
                    if (!splitClassAd.evaluateAttrInt("Year", int64)) {
                        time_components.setYear(0);
                    } else {
                        time_components.setYear((int) int64.getLongValue());
                    }
                }

                // The second argument, if provided, must be a string
                if (number_of_args == 1) {
                    format = "EEE MMM dd HH:mm:ss yyyy";
                    make_formatted_time(time_components, format, result);
                } else {
                    if (!argList.get(1).publicEvaluate(state, format_arg)) {
                        did_eval = false;
                    } else {
                        AMutableCharArrayString formatString = objectPool.strPool.get();
                        if (!format_arg.isStringValue(formatString)) {
                            result.setErrorValue();
                        } else {
                            make_formatted_time(time_components, formatString.toString(), result);
                        }
                    }
                }
            } else {
                result.setErrorValue();
            }
            if (!did_eval) {
                result.setErrorValue();
            }
            return did_eval;
        }
    };

    public static void make_formatted_time(ClassAdTime time_components, String format, Value result) {
        //replace c++ format elements with java elements
        format = format.replace("%m", "MM");
        format = format.replace("%d", "dd");
        format = format.replace("%Y", "yyyy");
        format = format.replace("%M", "mm");
        format = format.replace("%S", "ss");
        format = format.replace("%A", "EEEE");
        format = format.replace("%a", "EEE");
        format = format.replace("%B", "MMMM");
        format = format.replace("%b", "MMM");
        format = format.replace("%H", "HH");
        format = format.replace("%Y", "y");
        format = format.replace("%", "");
        DateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        result.setStringValue(df.format(time_components.getCalendar().getTime()));
    }

    public static final ClassAdFunc getField = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            ClassAdTime asecs = objectPool.classAdTimePool.get();
            ClassAdTime rsecs = objectPool.classAdTimePool.get();
            ClassAdTime clock = objectPool.classAdTimePool.get();
            ClassAdTime tms = objectPool.classAdTimePool.get();

            if (argList.size() != 1) {
                val.setErrorValue();
                return (true);
            }

            if (!argList.get(0).publicEvaluate(state, arg)) {
                val.setErrorValue();
                return false;
            }
            if (arg.isAbsoluteTimeValue(asecs)) {
                clock.setValue(asecs);
                Util.getLocalTime(clock, tms);
                switch (name) {

                    case "getyear":
                        // tm_year is years since 1900 --- make it y2k compliant :-)
                        val.setIntegerValue(tms.getYear());
                        break;
                    case "getmonth":
                        val.setIntegerValue(tms.getMonth() + 1);
                        break;
                    case "getdayofyear":
                        val.setIntegerValue(tms.getDayOfYear());
                        break;
                    case "getdayofmonth":
                        val.setIntegerValue(tms.getDayOfMonth());
                        break;
                    case "getdayofweek":
                        val.setIntegerValue(tms.getDayOfWeek());
                        break;
                    case "gethours":
                        val.setIntegerValue(tms.getHours());
                        break;
                    case "getminutes":
                        val.setIntegerValue(tms.getMinutes());
                        break;
                    case "getseconds":
                        val.setIntegerValue(tms.getSeconds());
                        break;
                    case "getdays":
                    case "getuseconds":
                        // not meaningful for abstimes
                        val.setErrorValue();
                        return true;
                    default:
                        throw new HyracksDataException("Should not reach here");
                }
                return (true);
            } else if (arg.isRelativeTimeValue(rsecs)) {
                switch (name.toLowerCase()) {
                    case "getyear":
                    case "getmonth":
                    case "getdayofmonth":
                    case "getdayofweek":
                    case "getdayofyear":
                        // not meaningful for reltimes
                        val.setErrorValue();
                        return true;
                    case "getdays":
                        val.setIntegerValue(rsecs.getRelativeTime() / 86400);
                        break;
                    case "gethours":
                        val.setIntegerValue((rsecs.getRelativeTime() % 86400) / 3600);
                        break;
                    case "getminutes":
                        val.setIntegerValue((rsecs.getRelativeTime() % 3600) / 60);
                        break;
                    case "getseconds":
                        val.setIntegerValue(rsecs.getRelativeTime() % 60);
                        break;
                    default:
                        throw new HyracksDataException("Should not reach here");
                }
                return true;
            }

            val.setErrorValue();
            return (true);
        }

    };
    public static final ClassAdFunc currentTime = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            // no arguments
            if (argList.size() > 0) {
                val.setErrorValue();
                return (true);
            }
            Literal time_literal = Literal.createAbsTime(objectPool.classAdTimePool.get(), objectPool);
            time_literal.GetValue(val);
            return true;
        }
    };
    public static final ClassAdFunc splitTime = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            ClassAd split = objectPool.classAdPool.get();

            if (argList.size() != 1) {
                result.setErrorValue();
                return true;
            }

            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return false;
            }

            if (!arg.isClassAdValue() && doSplitTime(arg, split, objectPool)) {
                result.setClassAdValue(split);
            } else {
                result.setErrorValue();
            }
            return true;
        }

    };

    public static boolean doSplitTime(Value time, ClassAd splitClassAd, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        boolean did_conversion;
        AMutableInt64 integer = objectPool.int64Pool.get();
        integer.setValue(0);
        AMutableDouble real = objectPool.doublePool.get();
        real.setValue(0);
        ClassAdTime asecs = objectPool.classAdTimePool.get();
        ClassAdTime rsecs = objectPool.classAdTimePool.get();
        ClassAd classad = objectPool.classAdPool.get();
        did_conversion = true;
        if (time.isIntegerValue(integer)) {
            asecs.setValue(integer.getLongValue());
            asecs.makeLocalAbsolute();
            absTimeToClassAd(asecs, splitClassAd);
        } else if (time.isRealValue(real)) {
            asecs.setValue((long) real.getDoubleValue());
            asecs.makeAbsolute(true);
            absTimeToClassAd(asecs, splitClassAd);
        } else if (time.isAbsoluteTimeValue(asecs)) {
            absTimeToClassAd(asecs, splitClassAd);
        } else if (time.isRelativeTimeValue(rsecs)) {
            relTimeToClassAd((rsecs.getRelativeTime() / 1000.0), splitClassAd);
        } else if (time.isClassAdValue(classad)) {
            splitClassAd = objectPool.classAdPool.get();
            splitClassAd.copyFrom(classad);
        } else {
            did_conversion = false;
        }
        return did_conversion;
    }

    public static void relTimeToClassAd(double rsecs, ClassAd splitClassAd) throws HyracksDataException {
        int days, hrs, mins;
        double secs;
        boolean is_negative;

        if (rsecs < 0) {
            rsecs = -rsecs;
            is_negative = true;
        } else {
            is_negative = false;
        }
        days = (int) rsecs;
        hrs = days % 86400;
        mins = hrs % 3600;
        secs = (mins % 60) + (rsecs - Math.floor(rsecs));
        days = days / 86400;
        hrs = hrs / 3600;
        mins = mins / 60;
        if (is_negative) {
            if (days > 0) {
                days = -days;
            } else if (hrs > 0) {
                hrs = -hrs;
            } else if (mins > 0) {
                mins = -mins;
            } else {
                secs = -secs;
            }
        }
        splitClassAd.insertAttr("Type", "RelativeTime");
        splitClassAd.insertAttr("Days", days);
        splitClassAd.insertAttr("Hours", hrs);
        splitClassAd.insertAttr("Minutes", mins);
        splitClassAd.insertAttr("Seconds", secs);
        return;
    }

    public static void absTimeToClassAd(ClassAdTime asecs, ClassAd splitClassAd) throws HyracksDataException {
        ClassAdTime tms = asecs.getGMTCopy();
        splitClassAd.insertAttr("Type", "AbsoluteTime");
        splitClassAd.insertAttr("Year", tms.getYear());
        splitClassAd.insertAttr("Month", tms.getMonth() + 1);
        splitClassAd.insertAttr("Day", tms.getDayOfMonth());
        splitClassAd.insertAttr("Hours", tms.getHours());
        splitClassAd.insertAttr("Minutes", tms.getMinutes());
        splitClassAd.insertAttr("Seconds", tms.getSeconds());
        // Note that we convert the timezone from seconds to minutes.
        splitClassAd.insertAttr("Offset", asecs.getOffset() / 1000);
        return;
    }

    public static final ClassAdFunc dayTime = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            val.setRelativeTimeValue(objectPool.classAdTimePool.get());
            return (true);
        }
    };
    public static final ClassAdFunc epochTime = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value val, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            // no arguments
            if (argList.size() > 0) {
                val.setErrorValue();
                return (true);
            }
            val.setIntegerValue(0);
            return (true);
        }
    };
    public static final ClassAdFunc strCat = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            AMutableCharArrayString buf = objectPool.strPool.get();
            AMutableCharArrayString s = objectPool.strPool.get();
            boolean errorFlag = false;
            boolean undefFlag = false;
            boolean rval = false;

            Value val = objectPool.valuePool.get();
            Value stringVal = objectPool.valuePool.get();

            for (int i = 0; i < argList.size(); i++) {

                s.reset();
                if (!(rval = argList.get(i).publicEvaluate(state, val))) {
                    break;
                }

                if (val.isStringValue(s)) {
                    buf.appendString(s);
                } else {
                    Value.convertValueToStringValue(val, stringVal, objectPool);
                    if (stringVal.isUndefinedValue()) {
                        undefFlag = true;
                        break;
                    } else if (stringVal.isErrorValue()) {
                        errorFlag = true;
                        result.setErrorValue();
                        break;
                    } else if (stringVal.isStringValue(s)) {
                        buf.appendString(s);
                    } else {
                        errorFlag = true;
                        break;
                    }
                }
            }

            // failed evaluating some argument
            if (!rval) {
                result.setErrorValue();
                return (false);
            }
            // type error
            if (errorFlag) {
                result.setErrorValue();
                return (true);
            }
            // some argument was undefined
            if (undefFlag) {
                result.setUndefinedValue();
                return (true);
            }

            result.setStringValue(buf);
            return (true);
        }
    };
    public static final ClassAdFunc changeCase = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value val = objectPool.valuePool.get();
            Value stringVal = objectPool.valuePool.get();
            AMutableCharArrayString str = objectPool.strPool.get();
            boolean lower = name.equalsIgnoreCase("tolower");
            int len;

            // only one argument
            if (argList.size() != 1) {
                result.setErrorValue();
                return true;
            }

            // check for evaluation failure
            if (!argList.get(0).publicEvaluate(state, val)) {
                result.setErrorValue();
                return false;
            }

            if (!val.isStringValue(str)) {
                Value.convertValueToStringValue(val, stringVal, objectPool);
                if (stringVal.isUndefinedValue()) {
                    result.setUndefinedValue();
                    return true;
                } else if (stringVal.isErrorValue()) {
                    result.setErrorValue();
                    return true;
                } else if (!stringVal.isStringValue(str)) {
                    result.setErrorValue();
                    return true;
                }
            }
            len = str.size();
            for (int i = 0; i <= len; i++) {
                str.setChar(i, lower ? Character.toLowerCase(str.charAt(i)) : Character.toUpperCase(str.charAt(i)));
            }
            result.setStringValue(str);
            return (true);
        }
    };
    public static final ClassAdFunc subString = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg0 = objectPool.valuePool.get();
            Value arg1 = objectPool.valuePool.get();
            Value arg2 = objectPool.valuePool.get();
            AMutableCharArrayString buf = objectPool.strPool.get();
            AMutableInt64 offset = objectPool.int64Pool.get();
            offset.setValue(0);
            AMutableInt64 len = objectPool.int64Pool.get();
            len.setValue(0);
            AMutableInt64 alen = objectPool.int64Pool.get();
            len.setValue(0);

            // two or three arguments
            if (argList.size() < 2 || argList.size() > 3) {
                result.setErrorValue();
                return (true);
            }

            // Evaluate all arguments
            if (!argList.get(0).publicEvaluate(state, arg0) || !argList.get(1).publicEvaluate(state, arg1)
                    || (argList.size() > 2 && !argList.get(2).publicEvaluate(state, arg2))) {
                result.setErrorValue();
                return (false);
            }

            // strict on undefined
            if (arg0.isUndefinedValue() || arg1.isUndefinedValue() || (argList.size() > 2 && arg2.isUndefinedValue())) {
                result.setUndefinedValue();
                return (true);
            }

            // arg0 must be string, arg1 must be int, arg2 (if given) must be int
            if (!arg0.isStringValue(buf) || !arg1.isIntegerValue(offset)
                    || (argList.size() > 2 && !arg2.isIntegerValue(len))) {
                result.setErrorValue();
                return (true);
            }

            // perl-like substr; negative offsets and lengths count from the end
            // of the string
            alen.setValue(buf.size());
            if (offset.getLongValue() < 0) {
                offset.setValue(alen.getLongValue() + offset.getLongValue());
            } else if (offset.getLongValue() >= alen.getLongValue()) {
                offset.setValue(alen.getLongValue());
            }
            if (len.getLongValue() <= 0) {
                len.setValue(alen.getLongValue() - offset.getLongValue() + len.getLongValue());
                if (len.getLongValue() < 0) {
                    len.setValue(0);
                }
            } else if (len.getLongValue() > alen.getLongValue() - offset.getLongValue()) {
                len.setValue(alen.getLongValue() - offset.getLongValue());
            }

            // to make sure that if length is specified as 0 explicitly
            // then, len is set to 0
            if (argList.size() == 3) {
                AMutableInt64 templen = objectPool.int64Pool.get();
                templen.setValue(0);
                arg2.isIntegerValue(templen);
                if (templen.getLongValue() == 0) {
                    len.setValue(0);
                }
            }
            result.setStringValue(buf.substr((int) offset.getLongValue(), (int) len.getLongValue()));
            return (true);
        }
    };
    public static final ClassAdFunc convInt = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            // takes exactly one argument
            if (argList.size() != 1) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return (false);
            }
            Value.convertValueToIntegerValue(arg, result, objectPool);
            return true;
        }
    };
    public static final ClassAdFunc compareString = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg0 = objectPool.valuePool.get();
            Value arg1 = objectPool.valuePool.get();
            Value arg0_s = objectPool.valuePool.get();
            Value arg1_s = objectPool.valuePool.get();

            // Must have two arguments
            if (argList.size() != 2) {
                result.setErrorValue();
                return (true);
            }

            // Evaluate both arguments
            if (!argList.get(0).publicEvaluate(state, arg0) || !argList.get(1).publicEvaluate(state, arg1)) {
                result.setErrorValue();
                return false;
            }

            // If either argument is undefined, then the result is
            // undefined.
            if (arg0.isUndefinedValue() || arg1.isUndefinedValue()) {
                result.setUndefinedValue();
                return true;
            }

            AMutableCharArrayString s0 = objectPool.strPool.get();
            AMutableCharArrayString s1 = objectPool.strPool.get();
            if (Value.convertValueToStringValue(arg0, arg0_s, objectPool)
                    && Value.convertValueToStringValue(arg1, arg1_s, objectPool) && arg0_s.isStringValue(s0)
                    && arg1_s.isStringValue(s1)) {

                int order;

                if (name.equalsIgnoreCase("strcmp")) {
                    order = s0.compareTo(s1);
                    if (order < 0) {
                        order = -1;
                    } else if (order > 0) {
                        order = 1;
                    }
                } else {
                    order = s0.compareToIgnoreCase(s1);
                    if (order < 0) {
                        order = -1;
                    } else if (order > 0) {
                        order = 1;
                    }
                }
                result.setIntegerValue(order);
            } else {
                result.setErrorValue();
            }

            return (true);
        }
    };
    public static final ClassAdFunc matchPattern = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            boolean have_options;
            Value arg0 = objectPool.valuePool.get();
            Value arg1 = objectPool.valuePool.get();
            Value arg2 = objectPool.valuePool.get();

            AMutableCharArrayString pattern = objectPool.strPool.get();
            AMutableCharArrayString target = objectPool.strPool.get();
            AMutableCharArrayString options_string = objectPool.strPool.get();

            // need two or three arguments: pattern, string, optional settings
            if (argList.size() != 2 && argList.size() != 3) {
                result.setErrorValue();
                return (true);
            }
            if (argList.size() == 2) {
                have_options = false;
            } else {
                have_options = true;
            }

            // Evaluate args
            if (!argList.get(0).publicEvaluate(state, arg0) || !argList.get(1).publicEvaluate(state, arg1)) {
                result.setErrorValue();
                return (false);
            }
            if (have_options && !argList.get(2).publicEvaluate(state, arg2)) {
                result.setErrorValue();
                return (false);
            }

            // if either arg is error, the result is error
            if (arg0.isErrorValue() || arg1.isErrorValue()) {
                result.setErrorValue();
                return (true);
            }
            if (have_options && arg2.isErrorValue()) {
                result.setErrorValue();
                return (true);
            }

            // if either arg is undefined, the result is undefined
            if (arg0.isUndefinedValue() || arg1.isUndefinedValue()) {
                result.setUndefinedValue();
                return (true);
            }
            if (have_options && arg2.isUndefinedValue()) {
                result.setUndefinedValue();
                return (true);
            } else if (have_options && !arg2.isStringValue(options_string)) {
                result.setErrorValue();
                return (true);
            }

            // if either argument is not a string, the result is an error
            if (!arg0.isStringValue(pattern) || !arg1.isStringValue(target)) {
                result.setErrorValue();
                return (true);
            }
            return regexp_helper(pattern.toString(), target, null, have_options, options_string.toString(), result);
        }
    };

    private static boolean regexp_helper(String pattern, AMutableCharArrayString target, String replace,
            boolean have_options, String options_string, Value result) {
        int options = 0;
        //pattern = pattern.replaceAll("");
        //pattern = pattern.replaceAll("?", ".");
        Pattern re;
        if (have_options) {
            // We look for the options we understand, and ignore
            // any others that we might find, hopefully allowing
            // forwards compatibility.
            if (options_string.contains("i")) {
                options |= Pattern.CASE_INSENSITIVE;
            }
        }
        // compile the patern
        re = Pattern.compile(pattern, options);
        Matcher matcher = re.matcher(target.toString());
        if (matcher.matches()) {
            result.setBooleanValue(true);
            return (true);
        } else {
            result.setBooleanValue(false);
            return (true);
        }
    }

    public static final ClassAdFunc matchPatternMember = null;
    public static final ClassAdFunc substPattern = null;
    public static final ClassAdFunc convReal = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();

            // takes exactly one argument
            if (argList.size() != 1) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return (false);
            }
            Value.convertValueToRealValue(arg, result, objectPool);
            return true;
        }
    };
    public static final ClassAdFunc convString = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();

            // takes exactly one argument
            if (argList.size() != 1) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return (false);
            }

            Value.convertValueToStringValue(arg, result, objectPool);
            return true;
        }
    };
    public static final ClassAdFunc unparse = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            if (argList.size() != 1 || argList.get(0).getKind() != NodeKind.ATTRREF_NODE) {
                result.setErrorValue();
            } else {

                // use the printpretty on arg0 to spew out
                PrettyPrint unp = objectPool.prettyPrintPool.get();
                AMutableCharArrayString szAttribute = objectPool.strPool.get();
                AMutableCharArrayString szValue = objectPool.strPool.get();
                ExprTree pTree;

                unp.unparse(szAttribute, argList.get(0));
                // look them up argument within context of the ad.
                if (state.getCurAd() != null && (pTree = state.getCurAd().lookup(szAttribute.toString())) != null) {
                    unp.unparse(szValue, pTree);
                }

                result.setStringValue(szValue);
            }

            return (true);
        }
    };
    public static final ClassAdFunc convBool = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            // takes exactly one argument
            if (argList.size() != 1) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return (false);
            }

            switch (arg.getType()) {
                case UNDEFINED_VALUE:
                    result.setUndefinedValue();
                    return (true);

                case ERROR_VALUE:
                case CLASSAD_VALUE:
                case LIST_VALUE:
                case SLIST_VALUE:
                case ABSOLUTE_TIME_VALUE:
                    result.setErrorValue();
                    return (true);

                case BOOLEAN_VALUE:
                    result.setValue(arg);
                    return (true);

                case INTEGER_VALUE: {
                    AMutableInt64 ival = objectPool.int64Pool.get();
                    ival.setValue(0);
                    arg.isIntegerValue(ival);
                    result.setBooleanValue(ival.getLongValue() != 0);
                    return (true);
                }

                case REAL_VALUE: {
                    AMutableDouble rval = objectPool.doublePool.get();
                    arg.isRealValue(rval);
                    result.setBooleanValue(rval.getDoubleValue() != 0.0);
                    return (true);
                }

                case STRING_VALUE: {
                    AMutableCharArrayString buf = objectPool.strPool.get();
                    arg.isStringValue(buf);
                    if (buf.equalsIgnoreCase("false") || buf.size() == 0) {
                        result.setBooleanValue(false);
                    } else {
                        result.setBooleanValue(true);
                    }
                    return (true);
                }

                case RELATIVE_TIME_VALUE: {
                    ClassAdTime rsecs = objectPool.classAdTimePool.get();
                    arg.isRelativeTimeValue(rsecs);
                    result.setBooleanValue(rsecs.getTimeInMillis() != 0);
                    return (true);
                }

                default:
                    throw new HyracksDataException("Should not reach here");
            }
        }
    };
    public static final ClassAdFunc convTime = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            Value arg2 = objectPool.valuePool.get();
            boolean relative = name.equalsIgnoreCase("reltime");
            boolean secondarg = false; // says whether a 2nd argument exists
            AMutableInt64 arg2num = objectPool.int64Pool.get();
            arg2num.setValue(0);

            if (argList.size() == 0 && !relative) {
                // absTime with no arguments returns the current time.
                return currentTime.call(name, argList, state, result, objectPool);
            }
            if ((argList.size() < 1) || (argList.size() > 2)) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return (false);
            }
            if (argList.size() == 2) { // we have a 2nd argument
                secondarg = true;
                if (!argList.get(1).publicEvaluate(state, arg2)) {
                    result.setErrorValue();
                    return (false);
                }
                AMutableInt64 ivalue2 = objectPool.int64Pool.get();
                ivalue2.setValue(0);
                AMutableDouble rvalue2 = objectPool.doublePool.get();
                ClassAdTime rsecs = objectPool.classAdTimePool.get();
                if (relative) {// 2nd argument is N/A for reltime
                    result.setErrorValue();
                    return (true);
                }
                // 2nd arg should be integer, real or reltime
                else if (arg2.isIntegerValue(ivalue2)) {
                    arg2num.setValue(ivalue2.getLongValue());
                } else if (arg2.isRealValue(rvalue2)) {
                    arg2num.setValue((long) rvalue2.getDoubleValue());
                } else if (arg2.isRelativeTimeValue(rsecs)) {
                    arg2num.setValue(rsecs.getTimeInMillis());
                } else {
                    result.setErrorValue();
                    return (true);
                }
            } else {
                secondarg = false;
                arg2num.setValue(0);
            }

            switch (arg.getType()) {
                case UNDEFINED_VALUE:
                    result.setUndefinedValue();
                    return (true);

                case ERROR_VALUE:
                case CLASSAD_VALUE:
                case LIST_VALUE:
                case SLIST_VALUE:
                case BOOLEAN_VALUE:
                    result.setErrorValue();
                    return (true);

                case INTEGER_VALUE: {
                    AMutableInt64 ivalue = objectPool.int64Pool.get();
                    ivalue.setValue(0);
                    arg.isIntegerValue(ivalue);
                    if (relative) {
                        ClassAdTime time = objectPool.classAdTimePool.get();
                        time.setRelativeTime(ivalue.getLongValue());
                        result.setRelativeTimeValue(time);
                    } else {
                        ClassAdTime atvalue = objectPool.classAdTimePool.get();
                        atvalue.setValue(ivalue.getLongValue());
                        if (secondarg) {
                            atvalue.setTimeZone((int) arg2num.getLongValue());
                        } else {
                            // the default offset is the current timezone
                            atvalue.setTimeZone(Literal.findOffset(atvalue));
                        }

                        if (atvalue.getOffset() == -1) {
                            result.setErrorValue();
                            return (false);
                        } else {
                            result.setAbsoluteTimeValue(atvalue);
                        }
                    }
                    return (true);
                }

                case REAL_VALUE: {
                    AMutableDouble rvalue = objectPool.doublePool.get();
                    arg.isRealValue(rvalue);
                    if (relative) {
                        ClassAdTime time = objectPool.classAdTimePool.get();
                        time.setRelativeTime((long) (1000 * rvalue.getDoubleValue()));
                        result.setRelativeTimeValue(time);
                    } else {
                        ClassAdTime atvalue = objectPool.classAdTimePool.get();
                        atvalue.setValue((long) rvalue.getDoubleValue());
                        if (secondarg) {
                            atvalue.setTimeZone((int) arg2num.getLongValue());
                        } else {
                            // the default offset is the current timezone
                            atvalue.setTimeZone(Literal.findOffset(atvalue));
                        }
                        result.setAbsoluteTimeValue(atvalue);
                    }
                    return (true);
                }

                case STRING_VALUE: {
                    //should'nt come here
                    // a string argument to this function is transformed to a literal directly
                }

                case ABSOLUTE_TIME_VALUE: {
                    ClassAdTime secs = objectPool.classAdTimePool.get();
                    arg.isAbsoluteTimeValue(secs);
                    if (relative) {
                        result.setRelativeTimeValue(secs);
                    } else {
                        result.setValue(arg);
                    }
                    return (true);
                }
                case RELATIVE_TIME_VALUE: {
                    if (relative) {
                        result.setValue(arg);
                    } else {
                        ClassAdTime secs = objectPool.classAdTimePool.get();
                        arg.isRelativeTimeValue(secs);
                        ClassAdTime atvalue = objectPool.classAdTimePool.get();
                        atvalue.setValue(secs);
                        if (secondarg) {
                            atvalue.setTimeZone((int) arg2num.getLongValue());
                        } else {
                            // the default offset is the current timezone
                            atvalue.setTimeZone(Literal.findOffset(atvalue));
                        }
                        result.setAbsoluteTimeValue(atvalue);
                    }
                    return (true);
                }

                default:
                    throw new HyracksDataException("Should not reach here");
            }
        }
    };
    public static final ClassAdFunc doRound = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            Value realValue = objectPool.valuePool.get();
            // takes exactly one argument
            if (argList.size() != 1) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return (false);
            }
            if (arg.getType() == ValueType.INTEGER_VALUE) {
                result.setValue(arg);
            } else {
                if (!Value.convertValueToRealValue(arg, realValue, objectPool)) {
                    result.setErrorValue();
                } else {
                    AMutableDouble rvalue = objectPool.doublePool.get();
                    realValue.isRealValue(rvalue);
                    if (name.equalsIgnoreCase("floor")) {
                        result.setIntegerValue((long) Math.floor(rvalue.getDoubleValue()));
                    } else if (name.equalsIgnoreCase("ceil") || name.equalsIgnoreCase("ceiling")) {
                        result.setIntegerValue((long) Math.ceil(rvalue.getDoubleValue()));
                    } else if (name.equalsIgnoreCase("round")) {
                        result.setIntegerValue(Math.round(rvalue.getDoubleValue()));
                    } else {
                        result.setErrorValue();
                    }
                }
            }
            return true;
        }
    };
    public static final ClassAdFunc doMath2 = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            Value arg2 = objectPool.valuePool.get();

            // takes 2 arguments  pow(val,base)
            if (argList.size() != 2) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg) || !argList.get(1).publicEvaluate(state, arg2)) {
                result.setErrorValue();
                return (false);
            }

            if (name.equalsIgnoreCase("pow")) {
                // take arg2 to the power of arg2
                AMutableInt64 ival = objectPool.int64Pool.get();
                ival.setValue(0);
                AMutableInt64 ibase = objectPool.int64Pool.get();
                ibase.setValue(0);
                if (arg.isIntegerValue(ival) && arg2.isIntegerValue(ibase) && ibase.getLongValue() >= 0) {
                    ival.setValue((long) (Math.pow(ival.getLongValue(), ibase.getLongValue())));
                    result.setIntegerValue(ival.getLongValue());
                } else {
                    Value realValue = objectPool.valuePool.get();
                    Value realBase = objectPool.valuePool.get();
                    if (!Value.convertValueToRealValue(arg, realValue, objectPool)
                            || !Value.convertValueToRealValue(arg2, realBase, objectPool)) {
                        result.setErrorValue();
                    } else {
                        AMutableDouble rvalue = objectPool.doublePool.get();
                        rvalue.setValue(0);
                        AMutableDouble rbase = objectPool.doublePool.get();
                        rbase.setValue(1);
                        realValue.isRealValue(rvalue);
                        realBase.isRealValue(rbase);
                        result.setRealValue(Math.pow(rvalue.getDoubleValue(), rbase.getDoubleValue()));
                    }
                }
            } else if (name.equalsIgnoreCase("quantize")) {
                // quantize arg1 to the next integral multiple of arg2
                // if arg2 is a list, choose the first item from the list that is larger than arg1
                // if arg1 is larger than all of the items in the list, the result is an error.

                Value val = objectPool.valuePool.get();
                Value base = objectPool.valuePool.get();
                if (!Value.convertValueToRealValue(arg, val, objectPool)) {
                    result.setErrorValue();
                } else {
                    // get the value to quantize into rval.
                    AMutableDouble rval = objectPool.doublePool.get();
                    AMutableDouble rbase = objectPool.doublePool.get();
                    val.isRealValue(rval);
                    if (arg2.isListValue()) {
                        ExprList list = objectPool.exprListPool.get();
                        arg2.isListValue(list);
                        base.setRealValue(0.0);
                        rbase.setValue(0.0); // treat an empty list as 'don't quantize'
                        for (ExprTree expr : list.getExprList()) {
                            if (!expr.publicEvaluate(state, base)) {
                                result.setErrorValue();
                                return false; // eval should not fail
                            }
                            if (Value.convertValueToRealValue(base, val, objectPool)) {
                                val.isRealValue(rbase);
                                if (rbase.getDoubleValue() >= rval.getDoubleValue()) {
                                    result.setValue(base);
                                    return true;
                                }
                            } else {
                                //TJ: should we ignore values that can't be converted?
                                result.setErrorValue();
                                return true;
                            }
                        }
                        // at this point base is the value of the last expression in the list.
                        // and rbase is the real value of it and rval > rbase.
                        // when this happens we want to quantize on multiples of the last
                        // list value, as if on a single value were passed rather than a list.
                        arg2.setValue(base);
                    } else {
                        // if arg2 is not a list, then it must evaluate to a real value
                        // or we can't use it. (note that if it's an int, we still want
                        // to return an int, but we assume that all ints can be converted to real)
                        if (!Value.convertValueToRealValue(arg2, base, objectPool)) {
                            result.setErrorValue();
                            return true;
                        }
                        base.isRealValue(rbase);
                    }

                    // at this point rbase should contain the real value of either arg2 or the
                    // last entry in the list. and rval should contain the value to be quantized.

                    AMutableInt64 ival = objectPool.int64Pool.get();
                    ival.setValue(0);
                    AMutableInt64 ibase = objectPool.int64Pool.get();
                    ibase.setValue(0);
                    if (arg2.isIntegerValue(ibase)) {
                        // quantize to an integer base,
                        if (ibase.getLongValue() == 0L) {
                            result.setValue(arg);
                        } else if (arg.isIntegerValue(ival)) {
                            ival.setValue(((ival.getLongValue() + ibase.getLongValue() - 1) / ibase.getLongValue())
                                    * ibase.getLongValue());
                            result.setIntegerValue(ival.getLongValue());
                        } else {
                            rval.setValue(
                                    Math.ceil(rval.getDoubleValue() / ibase.getLongValue()) * ibase.getLongValue());
                            result.setRealValue(rval);
                        }
                    } else {
                        double epsilon = 1e-8;
                        if (rbase.getDoubleValue() >= -epsilon && rbase.getDoubleValue() <= epsilon) {
                            result.setValue(arg);
                        } else {
                            // we already have the real-valued base in rbase so just use it here.
                            rval.setValue(
                                    Math.ceil(rval.getDoubleValue() / rbase.getDoubleValue()) * rbase.getDoubleValue());
                            result.setRealValue(rval);
                        }
                    }
                }
            } else {
                // unknown 2 argument math function
                result.setErrorValue();
            }
            return true;
        }
    };
    public static final ClassAdFunc random = new ClassAdFunc() {
        private Random randomGenerator = new Random(System.currentTimeMillis());

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            // takes exactly one argument
            if (argList.size() > 1) {
                result.setErrorValue();
                return (true);
            } else if (argList.size() == 0) {
                arg.setRealValue(1.0);
            } else if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return (false);
            }
            AMutableInt64 int_max = objectPool.int64Pool.get();
            int_max.setValue(0);
            AMutableDouble double_max = objectPool.doublePool.get();
            if (arg.isIntegerValue(int_max)) {
                int random_int = randomGenerator.nextInt((int) int_max.getLongValue());
                result.setIntegerValue(random_int);
            } else if (arg.isRealValue(double_max)) {
                double random_double = double_max.getDoubleValue() * randomGenerator.nextDouble();
                result.setRealValue(random_double);
            } else {
                result.setErrorValue();
            }

            return true;
        }
    };
    public static final ClassAdFunc ifThenElse = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg1 = objectPool.valuePool.get();
            MutableBoolean arg1_bool = objectPool.boolPool.get();
            // takes exactly three arguments
            if (argList.size() != 3) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg1)) {
                result.setErrorValue();
                return (false);
            }
            switch (arg1.getType()) {
                case BOOLEAN_VALUE:
                    if (!arg1.isBooleanValue(arg1_bool)) {
                        result.setErrorValue();
                        return (false);
                    }
                    break;
                case INTEGER_VALUE: {
                    AMutableInt64 intval = objectPool.int64Pool.get();
                    intval.setValue(0);
                    if (!arg1.isIntegerValue(intval)) {
                        result.setErrorValue();
                        return (false);
                    }
                    arg1_bool.setValue(intval.getLongValue() != 0L);
                    break;
                }
                case REAL_VALUE: {
                    AMutableDouble realval = objectPool.doublePool.get();
                    if (!arg1.isRealValue(realval)) {
                        result.setErrorValue();
                        return (false);
                    }
                    arg1_bool.setValue(realval.getDoubleValue() != 0.0);
                    break;
                }
                case UNDEFINED_VALUE:
                    result.setUndefinedValue();
                    return (true);
                case ERROR_VALUE:
                case CLASSAD_VALUE:
                case LIST_VALUE:
                case SLIST_VALUE:
                case STRING_VALUE:
                case ABSOLUTE_TIME_VALUE:
                case RELATIVE_TIME_VALUE:
                case NULL_VALUE:
                    result.setErrorValue();
                    return (true);
            }
            if (arg1_bool.booleanValue()) {
                if (!argList.get(1).publicEvaluate(state, result)) {
                    result.setErrorValue();
                    return (false);
                }
            } else {
                if (!argList.get(2).publicEvaluate(state, result)) {
                    result.setErrorValue();
                    return (false);
                }
            }
            return true;
        }
    };
    public static final ClassAdFunc stringListsIntersect = new ClassAdFunc() {

        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg0 = objectPool.valuePool.get();
            Value arg1 = objectPool.valuePool.get();
            Value arg2 = objectPool.valuePool.get();
            boolean have_delimiter;
            AMutableCharArrayString str0 = objectPool.strPool.get();
            AMutableCharArrayString str1 = objectPool.strPool.get();
            AMutableCharArrayString delimiter_string = objectPool.strPool.get();

            // need two or three arguments: pattern, list, optional settings
            if (argList.size() != 2 && argList.size() != 3) {
                result.setErrorValue();
                return true;
            }
            if (argList.size() == 2) {
                have_delimiter = false;
            } else {
                have_delimiter = true;
            }

            // Evaluate args
            if (!argList.get(0).publicEvaluate(state, arg0) || !argList.get(1).publicEvaluate(state, arg1)) {
                result.setErrorValue();
                return true;
            }
            if (have_delimiter && !argList.get(2).publicEvaluate(state, arg2)) {
                result.setErrorValue();
                return true;
            }

            // if either arg is error, the result is error
            if (arg0.isErrorValue() || arg1.isErrorValue()) {
                result.setErrorValue();
                return true;
            }
            if (have_delimiter && arg2.isErrorValue()) {
                result.setErrorValue();
                return true;
            }

            // if either arg is undefined, the result is undefined
            if (arg0.isUndefinedValue() || arg1.isUndefinedValue()) {
                result.setUndefinedValue();
                return true;
            }
            if (have_delimiter && arg2.isUndefinedValue()) {
                result.setUndefinedValue();
                return true;
            } else if (have_delimiter && !arg2.isStringValue(delimiter_string)) {
                result.setErrorValue();
                return true;
            }

            // if the arguments are not of the correct types, the result
            // is an error
            if (!arg0.isStringValue(str0) || !arg1.isStringValue(str1)) {
                result.setErrorValue();
                return true;
            }
            result.setBooleanValue(false);

            List<String> list0 = objectPool.stringArrayListPool.get();
            Set<String> set1 = new HashSet<>();

            split_string_list(str0, have_delimiter ? delimiter_string.charAt(0) : ',', list0);
            split_string_set(str1, have_delimiter ? delimiter_string.charAt(0) : ',', set1);

            for (String str : list0) {
                if (set1.contains(str)) {
                    result.setBooleanValue(true);
                    break;
                }
            }

            return true;
        }
    };
    public static final ClassAdFunc interval = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            Value intarg = objectPool.valuePool.get();
            AMutableInt64 tot_secs = objectPool.int64Pool.get();
            tot_secs.setValue(0);
            // takes exactly one argument
            if (argList.size() != 1) {
                result.setErrorValue();
                return (true);
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return (false);
            }
            if (!Value.convertValueToIntegerValue(arg, intarg, objectPool)) {
                result.setErrorValue();
                return (true);
            }
            if (!intarg.isIntegerValue(tot_secs)) {
                result.setErrorValue();
                return (true);
            }
            long days = tot_secs.getLongValue() / (3600 * 24);
            tot_secs.setValue(tot_secs.getLongValue() % (3600 * 24));
            long hours = tot_secs.getLongValue() / 3600;
            tot_secs.setValue(tot_secs.getLongValue() % 3600);
            long min = tot_secs.getLongValue() / 60;
            long secs = tot_secs.getLongValue() % 60;
            String strval;
            if (days != 0) {
                strval = String.format("%d+%02d:%02d:%02d", days, Math.abs(hours), Math.abs(min), Math.abs(secs));
            } else if (hours != 0) {
                strval = String.format("%d:%02d:%02d", hours, Math.abs(min), Math.abs(secs));
            } else if (min != 0) {
                strval = String.format("%d:%02d", min, Math.abs(secs));
            } else {
                strval = String.format("%d", secs);
            }
            result.setStringValue(strval);
            return true;
        }
    };
    public static final ClassAdFunc eval = new ClassAdFunc() {
        @Override
        public boolean call(String name, ExprList argList, EvalState state, Value result, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            Value arg = objectPool.valuePool.get();
            Value strarg = objectPool.valuePool.get();
            // takes exactly one argument
            if (argList.size() != 1) {
                result.setErrorValue();
                return true;
            }
            if (!argList.get(0).publicEvaluate(state, arg)) {
                result.setErrorValue();
                return false;
            }
            ClassAdParser parser = objectPool.classAdParserPool.get();
            ExprTreeHolder expr = objectPool.mutableExprPool.get();
            AMutableCharArrayString s = objectPool.strPool.get();
            if (!Value.convertValueToStringValue(arg, strarg, objectPool) || !strarg.isStringValue(s)) {
                result.setErrorValue();
                return true;
            }
            if (state.getDepthRemaining() <= 0) {
                result.setErrorValue();
                return false;
            }

            expr.reset();
            try {
                if (!parser.parseExpression(s.toString(), expr, true) || (expr.getInnerTree() == null)) {
                    result.setErrorValue();
                    return true;
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            state.decrementDepth();
            expr.setParentScope(state.getCurAd());
            boolean eval_ok = expr.publicEvaluate(state, result);
            state.incrementDepth();
            if (!eval_ok) {
                result.setErrorValue();
                return false;
            }
            return true;
        }
    };

    public static void split_string_list(AMutableCharArrayString amutableString, char delim, List<String> list) {
        if (amutableString.getLength() == 0) {
            return;
        }
        int index = 0;
        int lastIndex = 0;
        while (index < amutableString.getLength()) {
            index = amutableString.firstIndexOf(delim, lastIndex);
            if (index > 0) {
                list.add(amutableString.substr(lastIndex, index - lastIndex).trim());
                lastIndex = index + 1;
            } else {
                if (amutableString.getLength() > lastIndex) {
                    list.add(amutableString.substr(lastIndex).trim());
                }
                break;
            }
        }
    }

    public static void split_string_set(AMutableCharArrayString amutableString, char delim, Set<String> set) {
        if (amutableString.getLength() == 0) {
            return;
        }
        int index = 0;
        int lastIndex = 0;
        while (index < amutableString.getLength()) {
            index = amutableString.firstIndexOf(delim, lastIndex);
            if (index > 0) {
                set.add(amutableString.substr(lastIndex, index - lastIndex).trim());
                lastIndex = index + 1;
            } else {
                if (amutableString.getLength() > lastIndex) {
                    set.add(amutableString.substr(lastIndex).trim());
                }
                break;
            }
        }
    }
}
