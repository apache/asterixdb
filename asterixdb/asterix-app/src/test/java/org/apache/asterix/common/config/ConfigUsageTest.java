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
package org.apache.asterix.common.config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.asterix.hyracks.bootstrap.CCApplicationEntryPoint;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConfigUsageTest {

    private static final String CSV_FILE = FileUtil.joinPath("target", "surefire-reports", "config-options.csv");

    @Test
    public void generateUsage() {
        generateUsage("| ", " | ", " |", true, System.err);
    }

    @Test
    public void generateUsageCSV() throws IOException {
        new File(CSV_FILE).getParentFile().mkdirs();
        try (final PrintStream output = new PrintStream(new FileOutputStream(CSV_FILE))) {
            generateUsage("\"", "\",\"", "\"", false, output);
            // TODO(mblow): add some validation (in addition to just ensuring no exceptions...)
        }
    }

    public void generateUsage(String startDelim, String midDelim, String endDelim, boolean align, PrintStream output) {
        ConfigManager configManager = new ConfigManager();
        CCApplicationEntryPoint aep = new CCApplicationEntryPoint();
        aep.registerConfig(configManager);
        StringBuilder buf = new StringBuilder();
        int maxSectionWidth = 0;
        int maxNameWidth = 0;
        int maxDescriptionWidth = 0;
        int maxDefaultWidth = 0;
        if (align) {
            for (Section section : configManager.getSections()) {
                maxSectionWidth = Math.max(maxSectionWidth, section.sectionName().length());
                for (IOption option : configManager.getOptions(section)) {
                    if (option.hidden()) {
                        continue;
                    }
                    maxNameWidth = Math.max(maxNameWidth, option.ini().length());
                    maxDescriptionWidth = Math.max(maxDescriptionWidth,
                            option.description() == null ? 0 : option.description().length());
                    maxDefaultWidth = Math.max(maxDefaultWidth, configManager.defaultTextForUsage(option, IOption::ini)
                            .length());
                }
            }
        }
        maxDescriptionWidth = Math.min(80, maxDescriptionWidth);
        for (Section section : configManager.getSections()) {
            List<IOption> options = new ArrayList<>(configManager.getOptions(section));
            options.sort(Comparator.comparing(IOption::ini));
            for (IOption option : options) {
                if (option.hidden()) {
                    continue;
                }
                buf.append(startDelim);
                center(buf, section.sectionName(), maxSectionWidth).append(midDelim);
                pad(buf, option.ini(), maxNameWidth).append(midDelim);
                String description = option.description() == null ? "" : option.description();
                String defaultText = configManager.defaultTextForUsage(option, IOption::ini);
                boolean extra = false;
                while (align && description.length() > maxDescriptionWidth) {
                    int cut = description.lastIndexOf(' ', maxDescriptionWidth);
                    pad(buf, description.substring(0, cut), maxDescriptionWidth).append(midDelim);
                    pad(buf, defaultText, maxDefaultWidth).append(endDelim).append('\n');
                    defaultText = "";
                    description = description.substring(cut + 1);
                    buf.append(startDelim);
                    pad(buf, "", maxSectionWidth).append(midDelim);
                    pad(buf, "", maxNameWidth).append(midDelim);
                }
                pad(buf, description, maxDescriptionWidth).append(midDelim);
                pad(buf, defaultText, maxDefaultWidth).append(endDelim).append('\n');
                if (extra) {
                    buf.append(startDelim);
                    pad(buf, "", maxSectionWidth).append(midDelim);
                    pad(buf, "", maxNameWidth).append(midDelim);
                    pad(buf, "", maxDescriptionWidth).append(midDelim);
                    pad(buf, "", maxDefaultWidth).append(endDelim).append('\n');
                }
            }
        }
        output.println(buf);
    }

    private StringBuilder center(StringBuilder buf, String string, int width) {
        if (string == null) {
            string = "";
        }
        int pad = width - string.length();
        int leftPad = pad / 2;
        for (int i = leftPad; i > 0; i--) {
            buf.append(' ');
        }
        buf.append(string);
        for (int i = pad - leftPad; i > 0; i--) {
            buf.append(' ');
        }
        return buf;
    }

    private StringBuilder pad(StringBuilder buf, String string, int width) {
        if (string == null) {
            string = "";
        }
        buf.append(string);
        for (int i = width - string.length(); i > 0; i--) {
            buf.append(' ');
        }
        return buf;
    }

}
