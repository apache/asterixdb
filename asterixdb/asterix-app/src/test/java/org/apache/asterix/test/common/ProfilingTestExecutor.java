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
package org.apache.asterix.test.common;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;
import java.util.function.Predicate;

import org.apache.asterix.testframework.context.TestCaseContext;
import org.apache.asterix.testframework.xml.ParameterTypeEnum;
import org.apache.asterix.testframework.xml.TestCase;

public class ProfilingTestExecutor extends TestExecutor {

    private final TestCase.CompilationUnit.Parameter profile = new TestCase.CompilationUnit.Parameter();

    public InputStream executeQueryService(String str, TestCaseContext.OutputFormat fmt, URI uri,
            List<TestCase.CompilationUnit.Parameter> params, boolean jsonEncoded, Charset responseCharset,
            Predicate<Integer> responseCodeValidator, boolean cancellable) throws Exception {
        profile.setName("profile");
        profile.setValue("timings");
        profile.setType(ParameterTypeEnum.STRING);
        params.add(profile);
        return super.executeQueryService(str, fmt, uri, constructQueryParameters(str, fmt, params), jsonEncoded,
                responseCharset, responseCodeValidator, cancellable);

    }
}
