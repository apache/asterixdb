# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import math
import os

def sqrt(num):
    return math.sqrt(num)

class Tests(object):

    def roundtrip(self, *args):
        return args

    def roundstr(self, arg):
        return arg

    def codepoints(self, arg):
        return [ord(c) for c in arg]

    def warning(self):
        raise ArithmeticError("oof")

    def env_test(self, key):
        return os.environ[key]

    def binary_echo(self, arg):
        if not isinstance(arg, bytes):
            raise TypeError("expected bytes, got " + type(arg).__name__)
        return arg

    def binary_size(self, arg):
        if not isinstance(arg, bytes):
            raise TypeError("expected bytes, got " + type(arg).__name__)
        return len(arg)

    def binary_encode(self, arg):
        return arg.encode("utf-8")

    def binary_repeat(self, arg, times):
        return arg * times

    def binary_nested(self, arg):
        return {"value": arg, "list": [arg, arg]}
