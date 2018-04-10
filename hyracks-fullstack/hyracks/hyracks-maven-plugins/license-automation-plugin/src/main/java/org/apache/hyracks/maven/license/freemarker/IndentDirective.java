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
package org.apache.hyracks.maven.license.freemarker;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hyracks.maven.license.LicenseUtil;

import freemarker.core.Environment;
import freemarker.template.TemplateBooleanModel;
import freemarker.template.TemplateDirectiveBody;
import freemarker.template.TemplateDirectiveModel;
import freemarker.template.TemplateException;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateNumberModel;

public class IndentDirective implements TemplateDirectiveModel {

    private static final String PARAM_NAME_SPACES = "spaces";
    private static final String PARAM_NAME_UNPAD = "unpad";
    private static final String PARAM_NAME_WRAP = "wrap";
    private static final String PARAM_NAME_STRICT = "strict";

    @Override
    public void execute(Environment env, Map params, TemplateModel[] loopVars, TemplateDirectiveBody body)
            throws TemplateException, IOException {

        int numSpaces = -1;
        boolean unpad = false;
        boolean wrap = false;
        boolean strict = false;

        for (Object o : params.entrySet()) {
            Map.Entry ent = (Map.Entry) o;

            String paramName = (String) ent.getKey();
            TemplateModel paramValue = (TemplateModel) ent.getValue();

            switch (paramName) {
                case PARAM_NAME_SPACES:
                    numSpaces = getIntParam(paramName, paramValue);
                    break;
                case PARAM_NAME_UNPAD:
                    unpad = getBooleanParam(paramName, paramValue);
                    break;
                case PARAM_NAME_WRAP:
                    wrap = getBooleanParam(paramName, paramValue);
                    break;
                case PARAM_NAME_STRICT:
                    strict = getBooleanParam(paramName, paramValue);
                    break;
                default:
                    throw new TemplateModelException("Unsupported parameter: " + paramName);
            }
        }
        if (numSpaces < 0) {
            throw new TemplateModelException("The required \"" + PARAM_NAME_SPACES + "\" parameter is missing.");
        }

        if (body == null) {
            throw new TemplateModelException("Indent requires a body");
        } else {
            // Executes the nested body (same as <#nested> in FTL). In this
            // case we don't provide a special writer as the parameter:
            StringWriter sw = new StringWriter();
            body.render(sw);
            String fixedup = LicenseUtil.process(sw.toString(), unpad, wrap, strict);
            IOUtils.copy(new StringReader(fixedup), new IndentingWriter(env.getOut(), numSpaces));
        }
    }

    private int getIntParam(String paramName, TemplateModel paramValue) throws TemplateModelException {
        int spacesParam;
        if (!(paramValue instanceof TemplateNumberModel)) {
            throw paramException(paramName, "must be a number");
        }
        spacesParam = ((TemplateNumberModel) paramValue).getAsNumber().intValue();
        if (spacesParam < 0) {
            throw paramException(paramName, "can't be negative");
        }
        return spacesParam;
    }

    private boolean getBooleanParam(String paramName, TemplateModel paramValue) throws TemplateModelException {
        if (!(paramValue instanceof TemplateBooleanModel)) {
            throw paramException(paramName, "must be a boolean");
        }
        return ((TemplateBooleanModel) paramValue).getAsBoolean();
    }

    private TemplateModelException paramException(String paramName, String message) throws TemplateModelException {
        return new TemplateModelException("The '" + paramName + "' parameter " + message);
    }

    private static class IndentingWriter extends Writer {
        private final Writer out;
        private final char[] padChars;
        boolean needsToPad;

        public IndentingWriter(Writer out, int numSpaces) {
            this.out = out;
            padChars = new char[numSpaces];
            Arrays.fill(padChars, ' ');
            needsToPad = true;
        }

        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {
            for (int i = off; i < len; i++) {
                if (cbuf[i] == '\n') {
                    out.write(cbuf[i]);
                    needsToPad = true;
                } else {
                    if (needsToPad) {
                        out.write(padChars);
                        needsToPad = false;
                    }
                    out.write(cbuf[i]);
                }
            }
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.flush();
        }
    }
}
