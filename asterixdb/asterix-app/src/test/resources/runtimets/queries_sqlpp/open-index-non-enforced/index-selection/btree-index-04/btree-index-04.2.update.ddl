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

use test;

insert into TestOpen ({
    "c_id": 1,
    "c_x": 1,
    "c_s": "hello",
    "c_i64": 2,
    "c_i8": 2,
    "c_d": 2
});
insert into TestOpen ({
    "c_id": 2,
    "c_x": 2,
    "c_s": 2,
    "c_i64": "2",
    "c_i8": 2.5,
    "c_d": 3
});
insert into TestOpen ({
    "c_id": 3,
    "c_x": 3,
    "c_s": "world",
    "c_i64": 2,
    "c_i8": 4,
    "c_d": 3.125
});
insert into TestOpen ({
    "c_id": 4,
    "c_x": 4,
    "c_s": null,
    "c_i64": null,
    "c_i8": 500,
    "c_d": 3.25
});
insert into TestOpen ({
    "c_id": 5,
    "c_x": 5,
    "c_s": "hello",
    "c_i64": 2.25,
    "c_i8": 10000.25,
    "c_d": 3.5
});
insert into TestOpen ({
    "c_id": 6,
    "c_x": 6,
    "c_s": false,
    "c_i64": false,
    "c_i8": 2e100,
    "c_d": 2e100
});
insert into TestOpen ({
    "c_id": 7,
    "c_x": 7,
    "c_s": "world",
    "c_i64": 3
});
insert into TestOpen ({
    "c_id": 8,
    "c_x": 8
});
insert into TestOpen ({
    "c_id": 9,
    "c_x": 9,
    "c_d": 3.25
});
