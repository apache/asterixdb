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

## <a id="BitwiseFunctions">Bitwise Functions</a> ##

All Bit/Binary functions can only operate on 64-bit signed integers.

**Note:** All non-integer numbers and other data types result in null.

**Note:** The query language uses two’s complement representation.

When looking at the value in binary form, bit 1 is the Least Significant
Bit (LSB) and bit 32 is the Most Significant Bit (MSB).

(MSB) Bit 32 → `0000 0000 0000 0000 0000 0000 0000 0000` ← Bit 1 (LSB)

### bitand ###

* Syntax:

        BITAND(int_value1, int_value2, ... , int_valueN)

* Returns the result of a bitwise AND operation performed on all input
  integer values.

    The bitwise AND operation compares each bit of `int_value1` to the
    corresponding bit of every other `int_value`.
    If all bits are 1, then the corresponding result bit is set to 1;
    otherwise it is set to 0 (zero).

* Arguments:

    * `int_valueI`: Integers, or any valid expressions which evaluate to
      integers, that are used to compare.

* Return Value:

    * An integer, representing the bitwise AND between all of the input
      integers.

* Limitations:

    * Input values must be integers (such as 1 or 1.0) and cannot contain
      decimals (such as 1.2).

* Example 1:

    Compare 3 (0011 in binary) and 6 (0110 in binary).

        { "BitAND": BITAND(3,6) };

* The expected result is:

        { "BitAND": 2 }

    This results in 2 (0010 in binary) because only bit 2 is set in both 3
    (00**1**1) and 6 (01**1**0).

* Example 2:

    Compare 4.5 and 3 (0011 in binary).

        { "BitAND": BITAND(4.5,3) };

* The expected result is:

        { "BitAND": null }

    The result is null because 4.5 is not an integer.

* Example 3:

    Compare 4.0 (0100 in binary) and 3 (0011 in binary).

        { "BitAND": BITAND(4.0,3) };

* The expected result is:

        { "BitAND": 0 }

    This results in 0 (zero) because 4.0 (0100) and 3 (0011) do not share
    any bits that are both 1.

* Example 4:

    Compare 3 (0011 in binary) and 6 (0110 in binary) and 15 (1111 in binary).

        { "BitAND": BITAND(3,6,15) };

* The expected result is:

        { "BitAND": 2 }

    This results in 2 (0010 in binary) because only the 2nd bit from the
    right is 1 in all three numbers.

### bitclear ###

* Syntax:

        BITCLEAR(int_value, positions)

* Returns the result after clearing the specified bit, or array of bits in
  `int_value` using the given `positions`.

    **Note:** Specifying a negative or zero bit position makes the function
    return a null.

* Arguments:

    * `int_value`: An integer, or any valid expression which evaluates to an
      integer, that contains the target bit or bits to clear.

    * `positions`: An integer or an array of integers specifying the position
      or positions to be cleared.

* Return Value:

    * An integer, representing the result after clearing the bit or bits
      specified.

* Limitations:

    * Input values must be integers (such as 1 or 1.0) and cannot contain
      decimals (such as 1.2).

* Example 1:

    Clear bit 1 from 6 (0110 in binary).

        { "BitCLEAR": BITCLEAR(6,1) };

* The expected result is:

        { "BitCLEAR": 6 }

    This results in 6 (011**0** in binary) because bit 1 was already zero.

* Example 2:

    Clear bits 1 and 2 from 6 (01**10** in binary).

        { "BitCLEAR": BITCLEAR(6,[1,2]) };

* The expected result is:

        { "BitCLEAR": 4 }

    This results in 4 (01**0**0 in binary) because bit 2 changed to zero.

* Example 3:

    Clear bits 1, 2, 4, and 5 from 31 (0**11**1**11** in binary).

        { "BitCLEAR": BITCLEAR(31,[1,2,4,5]) };

* The expected result is:

        { "BitCLEAR": 4 }

    This results in 4 (0**00**1**00**) because bits 1, 2, 4, and 5 changed to
    zero.

### bitnot ###

* Syntax:

        BITNOT(int_value)

* Returns the results of a bitwise logical NOT operation performed on
  an integer value.

    The bitwise logical NOT operation reverses the bits in the value.
    For each value bit that is 1, the corresponding result bit will be
    set to 0 (zero); and for each value bit that is 0 (zero), the
    corresponding result bit will be set to 1.

    **Note:** All bits of the integer will be altered by this operation.

* Arguments:

    * `int_value`: An integer, or any valid expression which evaluates to an
      integer, that contains the target bits to reverse.

* Return Value:

    * An integer, representing the result after performing the logical NOT
      operation.

* Limitations:

    * Input values must be integers (such as 1 or 1.0) and cannot contain
      decimals (such as 1.2).

* Example 1:

    Perform the NOT operation on 3 (0000 0000 0000 0000 0000 0000 0000 0011 in binary).

        { "BitNOT": BITNOT(3) };

* The expected result is:

        { "BitNOT": -4 }

    This results in -4 (**1111 1111 1111 1111 1111 1111 1111 1100** in
    binary) because all bits changed.

### bitor ###

* Syntax:

        BITOR(int_value1, int_value2, ... , int_valueN)

* Returns the result of a bitwise inclusive OR operation performed on all input
  integer values.

    The bitwise inclusive OR operation compares each bit of `int_value1` to the
    corresponding bit of every other `int_value`.
    If any bit is 1, the corresponding result bit is set to 1; otherwise, it
    is set to 0 (zero).

* Arguments:

    * `int_valueI`: Integers, or any valid expressions which evaluate to
      integers, that are used to compare.

* Return Value:

    * An integer, representing the bitwise OR between all of the input
      integers.

* Limitations:

    * Input values must be integers (such as 1 or 1.0) and cannot contain
      decimals (such as 1.2).

* Example 1:

    Perform OR on 3 (0011 in binary) and 6 (0110 in binary).

        { "BitOR": BITOR(3,6) };

* The expected result is:

        { "BitOR": 7 }

    This results in 7 (0**111** in binary) because at least 1 bit of each
    (00**11** and 0**11**0) is 1 in bits 1, 2, and 3.

* Example 2:

    Perform OR on 3 (0011 in binary) and -4 (1000 0000 0000 ... 0000 1100 in
    binary).

        { "BitOR": BITOR(3,-4) };

* The expected result is:

        { "BitOR": -1 }

    This results in -1 (**1111 1111 1111 ... 1111 1111** in binary) because
    the two 1 bits in 3 fill in the two 0 bits in -4 to turn on all the bits.

* Example 3:

    Perform OR on 3 (0011 in binary) and 6 (0110 in binary) and 15 (1111 in
    binary).

        { "BitOR": BITOR(3,6,15) };

* The expected result is:

        { "BitOR": 15 }

    This results in 15 (1111 in binary) because there is at least one 1 in
    each of the four rightmost bits.

### bitset ###

* Syntax:

        BITSET(int_value, positions)

* Returns the result after setting the specified bit `position`, or
  array of bit positions, to 1 in the given `int_value`.

    **Note:** Specifying a negative or zero position makes the function return
    a null.

* Arguments:

    * `int_value`: An integer, or any valid expression which evaluates to an
      integer, that contains the target bit or bits to set.

    * `positions`: An integer or an array of integers specifying the position
      or positions to be set.

* Return Value:

    * An integer, representing the result after setting the bit or bits
      specified.
      If the bit is already set, then it stays set.

* Limitations:

    * Input values must be integers (such as 1 or 1.0) and cannot contain
      decimals (such as 1.2).

* Example 1:

    Set bit 1 in the value 6 (011**0** in binary).

        { "BitSET": BITSET(6,1) };

* The expected result is:

        { "BitSET": 7 }

    This results in 7 (011**1** in binary) because bit 1 changed to 1.

* Example 2:

    Set bits 1 and 2 in the value 6 (01**10** in binary).

        { "BitSET": BITSET(6,[1,2]) };

* The expected result is:

        { "BitSET": 7 }

    This also results in 7 (01**11** in binary) because bit 1 changed while
    bit 2 remained the same.

* Example 3:

    Set bits 1 and 4 in the value 6 (**0**11**0** in binary).

        { "BitSET": BITSET(6,[1,4]) };

* The expected result is:

        { "BitSET": 15 }

    This results in 15 (**1**11**1** in binary) because bit 1 and 4 changed
    to ones.

### bitshift ###

* Syntax:

        BITSHIFT(int_value, shift_amount[, rotate])

* Returns the result of a bit shift operation performed on the integer
  value `int_value`.
  The `shift_amount` supports left and right shifts.
  These are logical shifts.
  The third parameter `rotate` supports circular shift.
  This is similar to the BitROTATE function in Oracle.

* Arguments:

    * `int_value`: An integer, or any valid expression which evaluates to an
      integer, that contains the target bit or bits to shift.

    * `shift_amount`: An integer, or any valid expression which evaluates to an
      integer, that contains the number of bits to shift.

        - A positive (+) number means this is a LEFT shift.

        - A negative (-) number means this is a RIGHT shift.

    * `rotate`: (Optional) A boolean, or any valid expression which evaluates
      to a boolean, where:

        - FALSE means this is a LOGICAL shift, where bits shifted off
          the end of a value are considered lost.

        - TRUE means this is a CIRCULAR shift (shift-and-rotate
          operation), where bits shifted off the end of a value are
          rotated back onto the value at the *other* end.
          In other words, the bits rotate in what might be thought of as a
          circular pattern; therefore, these bits are not lost.

        If omitted, the default is FALSE.

    For comparison, see the below table.

    | Input | Shift | Result of Logical Shift (Rotate FALSE) | Result of Circular Shift (Rotate TRUE) |
    |-------------------|-------|-------------------|------------------------------------------------|
    | 6 (0000 0110)     | 4     | 96 (0110 0000)    | 96 (0110 0000)                                 |
    | 6 (0000 0110)     | 3     | 48 (0011 0000)    | 48 (0011 0000)                                 |
    | 6 (0000 0110)     | 2     | 24 (0001 1000)    | 24 (0001 1000)                                 |
    | 6 (0000 0110)     | 1     | 12 (0000 1100)    | 12 (0000 1100)                                 |
    | **6 (0000 0110)** | **0** | **6 (0000 0110)** | **6 (0000 0110)**                              |
    | 6 (0000 0110)     | -1    | 3 (0000 0011)     | 3 (0000 0011)                                  |
    | 6 (0000 0110)     | -2    | 1 (0000 0001)     | -9223372036854775807 (1000 0000 ... 0000 0001) |
    | 6 (0000 0110)     | -3    | 0 (0000 0000)     | -4611686018427387904 (1100 0000 ... 0000 0000) |
    | 6 (0000 0110)     | -4    | 0 (0000 0000)     | 6917529027641081856 (0110 0000 ... 0000 0000)  |

* Return Value:

    * An integer, representing the result of either a logical or circular
      shift of the given integer.

* Limitations:

    * Input values must be integers (such as 1 or 1.0) and cannot contain
      decimals (such as 1.2).

* Example 1:

    Logical left shift of the number 6 (0110 in binary) by one bit.

        { "BitSHIFT": BITSHIFT(6,1,FALSE) };

* The expected result is:

        { "BitSHIFT": 12 }

    This results in 12 (1100 in binary) because the 1-bits moved from
    positions 2 and 3 to positions 3 and 4.

* Example 2:

    Logical right shift of the number 6 (0110 in binary) by two bits.

        { "BitSHIFT": BITSHIFT(6,-2) };

* The expected result is:

        { "BitSHIFT": 1 }

    This results in 1 (0001 in binary) because the 1-bit in position 3 moved
    to position 1 and the 1-bit in position 2 was dropped.

* Example 2b:

    Circular right shift of the number 6 (0110 in binary) by two bits.

        { "BitSHIFT": BITSHIFT(6,-2,TRUE) };

* The expected result is:

        { "BitSHIFT": -9223372036854775807 }

    This results in -9223372036854775807 (1100 0000 0000 0000 0000 0000 0000
    0000 in binary) because the two 1-bits wrapped right, around to the Most
    Significant Digit position and changed the integer’s sign to negative.

* Example 3:

    Circular left shift of the number 524288 (1000 0000 0000 0000 0000 in
    binary) by 45 bits.

        { "BitSHIFT": BITSHIFT(524288,45,TRUE) };

* The expected result is:

        { "BitSHIFT": 1 }

    This results in 1 because the 1-bit wrapped left, around to the Least
    Significant Digit position.

### bittest ###

* Syntax:

        BITTEST(int_value, positions [, all_set])

* Returns TRUE if the specified bit, or bits, is a 1; otherwise,
  returns FALSE if the specified bit, or bits, is a 0 (zero).

    **Note:** Specifying a negative or zero bit position will result in null
    being returned.

* Arguments:

    * `int_value`: An integer, or any valid expression which evaluates to an
      integer, that contains the target bit or bits to test.

    * `positions`: An integer or an array of integers specifying the position
      or positions to be tested.

    * `all_set`: (Optional) A boolean, or any valid expression which evaluates
      to a boolean.

        - When `all_set` is FALSE, then it returns TRUE even if one bit in
          one of the positions is set.

        - When `all_set` is TRUE, then it returns TRUE only if all input
          positions are set.

        If omitted, the default is FALSE.

* Return Value:

    * A boolean, that follows the below table:

        | `int_value`                    | `all_set` | Return Value |
        |--------------------------------|-----------|--------------|
        | *all* specified bits are TRUE  | FALSE     | TRUE         |
        | *all* specified bits are TRUE  | TRUE      | TRUE         |
        | *some* specified bits are TRUE | FALSE     | TRUE         |
        | *some* specified bits are TRUE | TRUE      | FALSE        |

* Limitations:

    * Input values must be integers (such as 1 or 1.0) and cannot contain
      decimals (such as 1.2).

* Example 1:

    In the number 6 (0110 in binary), is bit 1 set?

        { "IsBitSET": ISBITSET(6,1) };

* The expected result is:

        { "IsBitSET": false }

    This returns FALSE because bit 1 of 6 (011**0** in binary) is not set to 1.

* Example 2:

    In the number 1, is either bit 1 or bit 2 set?

        { "BitTEST": BITTEST(1,[1,2],FALSE) };

* The expected result is:

        { "BitTEST": true }

    This returns TRUE because bit 1 of the number 1 (000**1** in binary) is
    set to 1.

* Example 3:

    In the number 6 (0110 in binary), are both bits 2 and 3 set?

        { "IsBitSET": ISBITSET(6,[2,3],TRUE) };

* The expected result is:

        { "IsBitSET": true }

    This returns TRUE because both bits 2 and 3 in the number 6 (0**11**0 in
    binary) are set to 1.

* Example 4:

    In the number 6 (0110 in binary), are all the bits in positions 1 through 3
    set?

        { "BitTEST": BITTEST(6,[1,3],TRUE) };

* The expected result is:

        { "BitTEST": false }

    This returns FALSE because bit 1 in the number 6 (011**0** in binary) is
    set to 0 (zero).

The function has an alias `isbitset`.

### bitxor ###

* Syntax:

        BITXOR(int_value1, int_value2, ... , int_valueN)

* Returns the result of a bitwise Exclusive OR operation performed on
  two or more integer values.

    The bitwise Exclusive OR operation compares each bit of `int_value1` to
    the corresponding bit of `int_value2`.

    If there are more than two input values, the first two are compared;
    then their result is compared to the next input value; and so on.

    When the compared bits do not match, the result bit is 1; otherwise,
    the compared bits do match, and the result bit is 0 (zero), as
    summarized:

    | Bit 1 | Bit 2 | XOR Result Bit |
    |-------|-------|----------------|
    | 0     | 0     | 0              |
    | 0     | 1     | 1              |
    | 1     | 0     | 1              |
    | 1     | 1     | 0              |

* Arguments:

    * `int_valueI`: Integers, or any valid expressions which evaluate to
      integers, that are used to compare.

* Return Value:

    * An integer, representing the bitwise XOR between the input
    integers.

* Limitations:

    * Input values must be integers (such as 1 or 1.0) and cannot contain
      decimals (such as 1.2).

* Example 1:

    Perform the XOR operation on 3 (0011 in binary) and 6 (0110 in binary).

        { "BitXOR": BITXOR(3,6) };

* The expected result is:

        { "BitXOR": 5 }

    This returns 5 (0101 in binary) because the 1st bit pair and 3rd bit
    pair are different (resulting in 1) while the 2nd bit pair and 4th bit
    pair are the same (resulting in 0):

        0011 (3)
        0110 (6)
        ====
        0101 (5)

* Example 2:

    Perform the XOR operation on 3 (0011 in binary) and 6 (0110 in binary) and
    15 (1111 in binary).

        { "BitXOR": BITXOR(3,6,15) };

* The expected result is:

        { "BitXOR": 10 }

    This returns 10 (1010 in binary) because 3 XOR 6 equals 5 (0101 in binary),
    and then 5 XOR 15 equals 10 (1010 in binary).
