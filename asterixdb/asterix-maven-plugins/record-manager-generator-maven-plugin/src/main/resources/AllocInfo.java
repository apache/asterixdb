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
package @PACKAGE@;

import java.io.PrintWriter;
import java.io.StringWriter;

public class AllocInfo {
    String alloc;
    String free;

    void alloc() {
        alloc = getStackTrace();
    }

    void free() {
        free = getStackTrace();
    }

    private String getStackTrace() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        new Exception().printStackTrace(pw);
        pw.close();
        String res = sw.toString();
        // remove first 3 lines
        int nlPos = 0;
        for (int i = 0; i < 3; ++i) {
            nlPos = res.indexOf('\n', nlPos) + 1;
        }
        return res.substring(nlPos);
    }

    public String toString() {
        return "allocation stack:\n" + alloc + "\nfree stack\n" + free;
    }
}
