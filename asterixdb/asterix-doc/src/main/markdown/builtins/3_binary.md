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

## <a id="BinaryFunctions">Binary Functions</a> ##
### parse_binary ###
  * Syntax:

      parse_binary(string, encoding)

  * Creates a `binary` from an string encoded in `encoding` format.
  * Arguments:
    * `string` : an encoded `string`,
    * `encoding` : a string notation specifies the encoding type of the given `string`.
       Currently we support `hex` and `base64` format.
  * Return Value:
    * a `binary` that is decoded from the given `string`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.

  * Example:

      [ parse_binary("ABCDEF0123456789","hex"), parse_binary("abcdef0123456789","HEX"), parse_binary('QXN0ZXJpeAE=',"base64") ];

  * The expected result is:

      [ hex("ABCDEF0123456789"), hex("ABCDEF0123456789"), hex("4173746572697801") ]

### print_binary ###
  * Syntax:

      print_binary(binary, encoding)

  * Prints a `binary` to the required encoding `string` format.
  * Arguments:
    * `binary` : a `binary` data need to be printed.
    * `encoding` : a string notation specifies the expected encoding type.
    Currently we support `hex` and `base64` format.
  * Return Value:
    * a `string` that represents the encoded format of a `binary`,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * any other non-string input value will cause a type error.

  * Example:

        [ print_binary(hex("ABCDEF0123456789"), "base64"), print_binary(base64("q83vASNFZ4k="), "hex") ]

  * The expected result are:

        [ "q83vASNFZ4k=", "ABCDEF0123456789" ]

### binary_length ###
  * Syntax:

      binary_length(binary)

  * Returns the number of bytes storing the binary data.
  * Arguments:
    * `binary` : a `binary` value to be checked.
  * Return Value:
    * an `bigint` that represents the number of bytes,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * any other non-binary input value will cause a type error.

  * Example:

        binary_length(hex("00AA"))

  * The expected result is:

       2

### sub_binary ###
  * Syntax:

      sub_binary(binary, offset[, length])

  * Returns the sub binary from the given `binary` based on the given start offset with the optional `length`.
  * Arguments:
    * `binary` : a `binary` to be extracted,
    * `offset` : a `tinyint`, `smallint`, `integer`, or `bigint` value
       as the starting offset of the sub binary in `binary` (starting at 0),
    * `length` : (Optional) a `tinyint`, `smallint`, `integer`, or `bigint` value
                  as the length of the sub binary.
  * Return Value:
    * a `binary` that represents the sub binary,
    * `missing` if any argument is a `missing` value,
    * `null` if any argument is a `null` value but no argument is a `missing` value,
    * a type error will be raised if:
        * the first argument is any other non-binary value,
        * or, the second argument is any other non-integer value,
        * or, the third argument is any other non-integer value, if it is present.

  * Example:

        sub_binary(hex("AABBCCDD"), 4);

  * The expected result is

        hex("DD")

### binary_concat ###
  * Syntax:

      binary_concat(array)

  * Concatenates a binary `array` or `multiset` into a single binary.
  * Arguments:
    * `array` : an `array` or `multiset` of binaries (could be `null` or `missing`) to be concatenated.
  * Return Value  :
    * the concatenated `binary` value,
    * `missing` if the argument is a `missing` value,
    * `null` if the argument is a `null` value,
    * `missing` if any element in the input array is `missing`,
    * `null` if any element in the input array is `null` but no element in the input array is `missing`,
    * any other non-array input value or non-binary element in the input array will cause a type error.

  * Example:

      binary_concat([hex("42"), hex(""), hex('42')]);

  * The expected result is

      hex("4242")

