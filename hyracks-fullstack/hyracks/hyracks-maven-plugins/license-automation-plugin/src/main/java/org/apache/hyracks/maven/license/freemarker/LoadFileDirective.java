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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hyracks.maven.license.LicenseUtil;

import freemarker.cache.FileTemplateLoader;
import freemarker.core.Environment;
import freemarker.template.Configuration;
import freemarker.template.TemplateBooleanModel;
import freemarker.template.TemplateDirectiveBody;
import freemarker.template.TemplateDirectiveModel;
import freemarker.template.TemplateException;
import freemarker.template.TemplateModel;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateScalarModel;

public class LoadFileDirective implements TemplateDirectiveModel {

    private static final String PARAM_FILE = "file";
    private static final String PARAM_TRIM = "trim";
    private static final String PARAM_DEFAULT_TEXT = "defaultOnMissing";

    @Override
    public void execute(Environment env, Map params, TemplateModel[] loopVars, TemplateDirectiveBody body)
            throws TemplateException, IOException {

        String fileParam = null;
        String defaultParam = null;
        boolean trimParam = false;

        for (Object paramObj : params.entrySet()) {
            Map.Entry param = (Map.Entry) paramObj;

            String paramName = (String) param.getKey();
            TemplateModel paramValue = (TemplateModel) param.getValue();

            switch (paramName) {
                case PARAM_FILE:
                    if (paramValue instanceof TemplateScalarModel) {
                        fileParam = ((TemplateScalarModel) paramValue).getAsString();
                    } else {
                        throw new TemplateModelException(PARAM_FILE + " must be a string");
                    }
                    break;

                case PARAM_DEFAULT_TEXT:
                    if (paramValue instanceof TemplateScalarModel) {
                        defaultParam = ((TemplateScalarModel) paramValue).getAsString();
                    } else {
                        throw new TemplateModelException(PARAM_DEFAULT_TEXT + " must be a string");
                    }
                    break;

                case PARAM_TRIM:
                    if (paramValue instanceof TemplateBooleanModel) {
                        trimParam = ((TemplateBooleanModel) paramValue).getAsBoolean();
                    } else {
                        throw new TemplateModelException(PARAM_TRIM + " must be a boolean");
                    }
                    break;

                default:
                    throw new TemplateModelException("Unknown param: " + paramName);
            }
        }
        if (fileParam == null) {
            throw new TemplateModelException("The required \"" + PARAM_FILE + "\" parameter" + "is missing.");
        }
        if (body != null) {
            throw new TemplateModelException("Body is not supported by this directive");
        }
        Writer out = env.getOut();
        File baseDir =
                ((FileTemplateLoader) ((Configuration) env.getTemplate().getParent()).getTemplateLoader()).baseDir;
        File file = new File(baseDir, fileParam);
        if (file.exists()) {
            if (trimParam) {
                LicenseUtil.readAndTrim(out, file);
                out.write('\n');
            } else {
                IOUtils.copy(new FileInputStream(file), out, StandardCharsets.UTF_8);
            }
        } else if (defaultParam != null) {
            out.append(defaultParam).append("\n");
        } else {
            throw new IOException("File not found: " + file);
        }
    }
}
