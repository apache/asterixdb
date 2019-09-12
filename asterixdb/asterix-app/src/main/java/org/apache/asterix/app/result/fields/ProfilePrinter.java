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
package org.apache.asterix.app.result.fields;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ProfilePrinter implements IResponseFieldPrinter {

    public static final String FIELD_NAME = "profile";
    private final ObjectNode profile;
    private final Logger LOGGER = LogManager.getLogger();

    public ProfilePrinter(ObjectNode profile) {
        this.profile = profile;
    }

    @Override
    public void print(PrintWriter pw) {
        boolean hasProfile = profile != null;
        if (hasProfile) {
            try {
                pw.print("\t\"" + FIELD_NAME + "\" : ");
                ObjectMapper om = new ObjectMapper();
                om.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
                DefaultIndenter ind = new DefaultIndenter("\t", DefaultIndenter.SYS_LF) {
                    @Override
                    public void writeIndentation(JsonGenerator jg, int level) throws IOException {
                        super.writeIndentation(jg, level + 1);
                    }
                };
                DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
                pp = pp.withArrayIndenter(ind);
                pp = pp.withObjectIndenter(ind);
                om.writer(pp).writeValue(pw, profile);
            } catch (IOException e) {
                LOGGER.error("Unable to print job profile", e);

            }
        }
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
