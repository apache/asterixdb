<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

## <a id="NumericFunctions">Numeric Functions</a> ##
### abs ###
 * Syntax:

        abs(numeric_value)

 * Computes the absolute value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * The absolute value of the argument with the same type as the input argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": abs(2013), "v2": abs(-4036), "v3": abs(0), "v4": abs(float("-2013.5")), "v5": abs(double("-2013.593823748327284")) };


 * The expected result is:

        { "v1": 2013, "v2": 4036, "v3": 0, "v4": 2013.5, "v5": 2013.5938237483274 }


### acos ###
 * Syntax:

        acos(numeric_value)

 * Computes the arc cosine value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` arc cosine in radians for the argument,
       if the argument is in the range of -1 (inclusive) to 1 (inclusive),
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error,
    * "NaN" for other legitimate numeric values.

 * Example:

        { "v1": acos(1), "v2": acos(2), "v3": acos(0), "v4": acos(float("0.5")), "v5": acos(double("-0.5")) };


 * The expected result is:

        { "v1": 0.0, "v2": "NaN", "v3": 1.5707963267948966, "v4": 1.0471975511965979, "v5": 2.0943951023931957 }



### asin ###
 * Syntax:

        asin(numeric_value)

 * Computes the arc sine value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` arc sin in radians for the argument,
       if the argument is in the range of -1 (inclusive) to 1 (inclusive),
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error,
    * "NaN" for other legitimate numeric values.

 * Example:

        { "v1": asin(1), "v2": asin(2), "v3": asin(0), "v4": asin(float("0.5")), "v5": asin(double("-0.5")) };


 * The expected result is:

        { "v1": 1.5707963267948966, "v2": "NaN", "v3": 0.0, "v4": 0.5235987755982989, "v5": -0.5235987755982989 }


### atan ###
 * Syntax:

        atan(numeric_value)

 * Computes the arc tangent value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` arc tangent in radians for the argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": atan(1), "v2": atan(2), "v3": atan(0), "v4": atan(float("0.5")), "v5": atan(double("1000")) };


 * The expected result is:

        { "v1": 0.7853981633974483, "v2": 1.1071487177940904, "v3": 0.0, "v4": 0.4636476090008061, "v5": 1.5697963271282298 }


### atan2 ###
 * Syntax:

        atan2(numeric_value1, numeric_value2)

 * Computes the arc tangent value of numeric_value2/numeric_value1.
 * Arguments:
    * `numeric_value1`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value,
    * `numeric_value2`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` arc tangent in radians for `numeric_value1` and `numeric_value2`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": atan2(1, 2), "v2": atan2(0, 4), "v3": atan2(float("0.5"), double("-0.5")) };


 * The expected result is:

        { "v1": 0.4636476090008061, "v2": 0.0, "v3": 2.356194490192345 }


### ceil ###
 * Syntax:

        ceil(numeric_value)

 * Computes the smallest (closest to negative infinity) number with no fractional part that is not less than the value of the argument. If the argument is already equal to mathematical integer, then the result is the same as the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * The ceiling value for the given number in the same type as the input argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        {
          "v1": ceil(2013),
          "v2": ceil(-4036),
          "v3": ceil(0.3),
          "v4": ceil(float("-2013.2")),
          "v5": ceil(double("-2013.893823748327284"))
        };


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0, "v4": -2013.0, "v5": -2013.0 }


### cos ###
 * Syntax:

        cos(numeric_value)

 * Computes the cosine value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` cosine value for the argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": cos(1), "v2": cos(2), "v3": cos(0), "v4": cos(float("0.5")), "v5": cos(double("1000")) };


 * The expected result is:

        { "v1": 0.5403023058681398, "v2": -0.4161468365471424, "v3": 1.0, "v4": 0.8775825618903728, "v5": 0.562379076290703 }


### cosh ###
 * Syntax:

        cosh(numeric_value)

 * Computes the hyperbolic cosine value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` hyperbolic cosine value for the argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": cosh(1), "v2": cosh(2), "v3": cosh(0), "v4": cosh(float("0.5")), "v5": cosh(double("8")) };


 * The expected result is:

        { "v1": 1.5430806348152437, "v2": 3.7621956910836314, "v3": 1.0, "v4": 1.1276259652063807, "v5": 1490.479161252178 }


### degrees ###
 * Syntax:

        degrees(numeric_value)

 * Converts radians to degrees
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * The degrees value for the given radians value. The returned value has type `double`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": degrees(pi()) };


 * The expected result is:

        { "v1": 180.0 }


### e ###
 * Syntax:

        e()

 * Return Value:
    * e (base of the natural logarithm)

 * Example:

        { "v1": e() };

 * The expected result is:

        { "v1": 2.718281828459045 }


### exp ###
 * Syntax:

        exp(numeric_value)

 * Computes e<sup>numeric_value</sup>.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * e<sup>numeric_value</sup>,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": exp(1), "v2": exp(2), "v3": exp(0), "v4": exp(float("0.5")), "v5": exp(double("1000")) };


 * The expected result is:

        { "v1": 2.718281828459045, "v2": 7.38905609893065, "v3": 1.0, "v4": 1.6487212707001282, "v5": "Infinity" }


### floor ###
 * Syntax:

        floor(numeric_value)

 * Computes the largest (closest to positive infinity) number with no fractional part that is not greater than the value.
   If the argument is already equal to mathematical integer, then the result is the same as the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * The floor value for the given number in the same type as the input argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        {
          "v1": floor(2013),
          "v2": floor(-4036),
          "v3": floor(0.8),
          "v4": floor(float("-2013.2")),
          "v5": floor(double("-2013.893823748327284"))
        };


 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 0.0, "v4": -2014.0, "v5": -2014.0 }


### ln ###
 * Syntax:

        ln(numeric_value)

 * Computes log<sub>e</sub>numeric_value.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * log<sub>e</sub>numeric_value,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": ln(1), "v2": ln(2), "v3": ln(0), "v4": ln(float("0.5")), "v5": ln(double("1000")) };


 * The expected result is:

        { "v1": 0.0, "v2": 0.6931471805599453, "v3": "-Infinity", "v4": -0.6931471805599453, "v5": 6.907755278982137 }



### log ###
 * Syntax:

        log(numeric_value)

 * Computes log<sub>10</sub>numeric_value.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * log<sub>10</sub>numeric_value,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": log(1), "v2": log(2), "v3": log(0), "v4": log(float("0.5")), "v5": log(double("1000")) };

 * The expected result is:

        { "v1": 0.0, "v2": 0.3010299956639812, "v3": "-Infinity", "v4": -0.3010299956639812, "v5": 3.0 }


### pi ###
 * Syntax:

        pi()

 * Return Value:
    * Pi

 * Example:

        { "v1": pi() };

 * The expected result is:

        { "v1": 3.141592653589793 }


### power ###
 * Syntax:

        power(numeric_value1, numeric_value2)

 * Computes numeric_value1<sup>numeric_value2</sup>.
 * Arguments:
    * `numeric_value1`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value,
    * `numeric_value2`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * numeric_value1<sup>numeric_value2</sup>,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": power(1, 2), "v3": power(0, 4), "v4": power(float("0.5"), double("-0.5")) };


 * The expected result is:

        { "v1": 1, "v3": 0, "v4": 1.4142135623730951 }


### radians ###
 * Syntax:

        radians(numeric_value)

 * Converts degrees to radians
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * The radians value for the given degrees value. The returned value has type `double`,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": radians(180) };


 * The expected result is:

        { "v1": 3.141592653589793 }


### round ###
 * Syntax:

        round(numeric_value[, round_digit])

 * Rounds the value to the given number of integer digits to the right of the decimal point,
   or to the left of the decimal point if the number of digits is negative.

 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value
      that represents the numeric value to be rounded.
    * `round_digit`: (Optional) a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value
      that specifies the digit to round to.
      This argument may be positive or negative;
      positive indicating that rounding needs to be to the right of the decimal point,
      and negative indicating that rounding needs to be to the left of the decimal point.
      Values such as 1.0 and 2.0 are acceptable, but values such as 1.3 and 1.5 result in a `null`.
      If omitted, the default is 0.
 * Return Value:
    * The rounded value for the given number.
      The returned value has the following type:
        - `bigint` if the input value has type `tinyint`, `smallint`, `integer` or `bigint`,
        - `float` if the input value has type `float`,
        - `double` if the input value has type `double`;
    * `missing` if the input value is a `missing` value,
    * `null` if the input value is a `null` value,
    * any other non-numeric input value will return a `null` value.

 * Example:

        {
          "v1": round(2013),
          "v2": round(-4036),
          "v3": round(0.8),
          "v4": round(float("-2013.256")),
          "v5": round(double("-2013.893823748327284"))
          "v6": round(123456, -1),
          "v7": round(456.456, 2),
          "v8": round(456.456, -1),
          "v9": round(-456.456, -2)
        };

 * The expected result is:

        { "v1": 2013, "v2": -4036, "v3": 1.0, "v4": -2013.0, "v5": -2014.0, "v6": 123460, "v7": 456.46, "v8": 460, "v9": -500 }


### sign ###
 * Syntax:

        sign(numeric_value)

 * Computes the sign of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the sign (a `tinyint`) of the argument, -1 for negative values, 0 for 0, and 1 for positive values,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": sign(1), "v2": sign(2), "v3": sign(0), "v4": sign(float("0.5")), "v5": sign(double("-1000")) };


 * The expected result is:

        { "v1": 1, "v2": 1, "v3": 0, "v4": 1, "v5": -1 }



### sin ###
 * Syntax:

        sin(numeric_value)

 * Computes the sine value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` sine value for the argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": sin(1), "v2": sin(2), "v3": sin(0), "v4": sin(float("0.5")), "v5": sin(double("1000")) };


 * The expected result is:

        { "v1": 0.8414709848078965, "v2": 0.9092974268256817, "v3": 0.0, "v4": 0.479425538604203, "v5": 0.8268795405320025 }


### sinh ###
 * Syntax:

        sinh(numeric_value)

 * Computes the hyperbolic sine value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` hyperbolic sine value for the argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": sinh(1), "v2": sinh(2), "v3": sinh(0), "v4": sinh(float("0.5")), "v5": sinh(double("8")) };


 * The expected result is:

        { "v1": 1.1752011936438014, "v2": 3.626860407847019, "v3": 0.0, "v4": 0.5210953054937474, "v5": 1490.4788257895502 }


### sqrt ###
 * Syntax:

        sqrt(numeric_value)

 * Computes the square root of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` square root value for the argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": sqrt(1), "v2": sqrt(2), "v3": sqrt(0), "v4": sqrt(float("0.5")), "v5": sqrt(double("1000")) };


 * The expected result is:

        { "v1": 1.0, "v2": 1.4142135623730951, "v3": 0.0, "v4": 0.7071067811865476, "v5": 31.622776601683793 }


### tan ###
 * Syntax:

        tan(numeric_value)

 * Computes the tangent value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` tangent value for the argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": tan(1), "v2": tan(2), "v3": tan(0), "v4": tan(float("0.5")), "v5": tan(double("1000")) };


 * The expected result is:

        { "v1": 1.5574077246549023, "v2": -2.185039863261519, "v3": 0.0, "v4": 0.5463024898437905, "v5": 1.4703241557027185 }


### tanh ###
 * Syntax:

        tanh(numeric_value)

 * Computes the hyperbolic tangent value of the argument.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value.
 * Return Value:
    * the `double` hyperbolic tangent value for the argument,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-numeric input value will cause a type error.

 * Example:

        { "v1": tanh(1), "v2": tanh(2), "v3": tanh(0), "v4": tanh(float("0.5")), "v5": tanh(double("8")) };


 * The expected result is:

        { "v1": 0.7615941559557649, "v2": 0.964027580075817, "v3": 0.0, "v4": 0.4621171572600098, "v5": 0.999999774929676 }


### trunc ###
 * Syntax:

        trunc(numeric_value, number_digits)

 * Truncates the number to the given number of integer digits to the right of the decimal point (left if digits is negative).
    Digits is 0 if not given.
 * Arguments:
    * `numeric_value`: a `tinyint`/`smallint`/`integer`/`bigint`/`float`/`double` value,
    * `number_digits`: a `tinyint`/`smallint`/`integer`/`bigint` value.
 * Return Value:
    * the `double` tangent value for the argument,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is `missing`,
    * a type error will be raised if:
        * the first argument is any other non-numeric value,
        * the second argument is any other non-tinyint, non-smallint, non-integer, and non-bigint value.

 * Example:

        { "v1": trunc(1, 1), "v2": trunc(2, -2), "v3": trunc(0.122, 2), "v4": trunc(float("11.52"), -1), "v5": trunc(double("1000.5252"), 3) };


 * The expected result is:

        { "v1": 1, "v2": 2, "v3": 0.12, "v4": 10.0, "v5": 1000.525 }

