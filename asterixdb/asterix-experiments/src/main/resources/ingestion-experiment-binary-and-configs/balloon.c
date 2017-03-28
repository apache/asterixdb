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

#include <stdio.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdint.h>

  int main() {
  
    void *mem;
    int err;
    uint64_t size = 4.5 * 1024 * 1024;
    size *= 1024;
  
    mem = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0); 
    printf ("%d\n", mem);
    err = mlock(mem, size);
    printf ("err is %d\n", err);
    while(1) {
      sleep(10000);
    }
    return 0;
  }

