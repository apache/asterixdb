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
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.hyracks.bootstrap.CCApplication;
import org.apache.asterix.hyracks.bootstrap.NCApplication;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.controllers.ControllerConfig;
import org.apache.hyracks.util.file.FileUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ConfigUsageTest {
    private ConfigManager configManager;
    final EnumMap<Column, Integer> maxWidths = new EnumMap<>(Column.class);

    protected enum Column {
        SECTION,
        PARAMETER,
        DESCRIPTION,
        DEFAULT
    }

    private static final String CSV_FILE = FileUtil.joinPath("target", "surefire-reports", "config-options.csv");

    @Before
    public void setup() {
        configManager = getConfigManager();
    }

    @Test
    public void generateCcUsage() {
        generateUsage(Section.CC);
    }

    @Test
    public void generateNcUsage() {
        generateUsage(Section.NC);
    }

    @Test
    public void generateCommonUsage() {
        generateUsage(Section.COMMON);
    }

    protected void generateUsage(Section... sections) {
        final EnumMap<Column, Boolean> align = new EnumMap<>(Column.class);
        align.put(Column.SECTION, true);
        align.put(Column.PARAMETER, true);
        System.err.println();
        generateUsage("| ", " | ", " |", align, System.err, sections);
    }

    @Test
    public void generateUsageCSV() throws IOException {
        new File(CSV_FILE).getParentFile().mkdirs();
        try (final PrintStream output = new PrintStream(new FileOutputStream(CSV_FILE))) {
            generateUsage("\"", "\",\"", "\"", new EnumMap<>(Column.class), output, getSections(configManager));
            // TODO(mblow): add some validation (in addition to just ensuring no exceptions...)
        }
    }

    protected ConfigManager getConfigManager() {
        ConfigManager configManager = new ConfigManager();
        new CCApplication().registerConfig(configManager);
        new NCApplication().registerConfig(configManager);
        ControllerConfig.Option.DEFAULT_DIR
                .setDefaultValue(((String) ControllerConfig.Option.DEFAULT_DIR.defaultValue())
                        .replace(System.getProperty("java.io.tmpdir"), "${java.io.tmpdir}/"));
        return configManager;
    }

    protected Section[] getSections(ConfigManager configManager) {
        TreeSet<Section> sections = new TreeSet<>(Comparator.comparing(Section::sectionName));
        sections.addAll(configManager.getSections());
        sections.remove(Section.LOCALNC);
        return sections.toArray(new Section[0]);
    }

    protected Predicate<IOption> optionSelector() {
        return o -> !o.hidden() && o != ControllerConfig.Option.CONFIG_FILE
                && o != ControllerConfig.Option.CONFIG_FILE_URL;
    }

    protected Set<IOption> getSectionOptions(ConfigManager configManager, Section section) {
        return configManager.getOptions(section).stream().filter(optionSelector()).collect(Collectors.toSet());
    }

    public void generateUsage(String startDelim, String midDelim, String endDelim, EnumMap<Column, Boolean> align,
            PrintStream output, Section... sections) {
        ConfigManager configManager = getConfigManager();
        StringBuilder buf = new StringBuilder();

        final Column[] columns = Column.values();
        for (Section section : getSections(configManager)) {
            for (IOption option : getSectionOptions(configManager, section)) {
                for (Column column : columns) {
                    if (align.computeIfAbsent(column, c -> false)) {
                        calculateMaxWidth(option, column);
                    }
                }
            }
        }
        // output header
        for (Column column : columns) {
            buf.append(column.ordinal() == 0 ? startDelim : midDelim);
            pad(buf, getColumnDisplayFunction().apply(column),
                    align.computeIfAbsent(column, c -> false) ? calculateMaxWidth(column, column.name()) : 0);
        }
        buf.append(endDelim).append('\n');

        StringBuilder sepLine = new StringBuilder();
        for (Column column : columns) {
            sepLine.append(column.ordinal() == 0 ? startDelim : midDelim);
            pad(sepLine, "", maxWidths.getOrDefault(column, 0), '-');
        }
        sepLine.append(endDelim).append('\n');
        buf.append(sepLine.toString().replace(' ', '-'));

        List<IOption> options = new ArrayList<>();

        for (Section section : sections) {
            options.addAll(getSectionOptions(configManager, section));
        }
        options.sort(Comparator.comparing(getIOptionNameDisplayFunction()));
        for (IOption option : options) {
            for (Column column : columns) {
                buf.append(column.ordinal() == 0 ? startDelim : midDelim);
                if (column == Column.SECTION) {
                    center(buf, extractValue(column, option), maxWidths.getOrDefault(column, 0));
                } else {
                    pad(buf, extractValue(column, option), maxWidths.getOrDefault(column, 0));
                }
            }
            buf.append(endDelim).append('\n');
        }
        output.println(buf);
    }

    protected Function<Column, String> getColumnDisplayFunction() {
        return column -> StringUtils.capitalize(column.name().toLowerCase());
    }

    protected Function<IOption, String> getIOptionNameDisplayFunction() {
        return IOption::ini;
    }

    protected Function<Section, String> getSectionDisplayFunction() {
        return Section::sectionName;
    }

    protected int calculateMaxWidth(IOption option, Column column) {
        final String string = extractValue(column, option);
        return calculateMaxWidth(column, string);
    }

    private int calculateMaxWidth(Column column, String string) {
        final int maxWidth = Math.max(maxWidths.computeIfAbsent(column, c -> 0), string.length());
        maxWidths.put(column, maxWidth);
        return maxWidth;
    }

    private String extractValue(Column column, IOption option) {
        switch (column) {
            case SECTION:
                return getSectionDisplayFunction().apply(option.section());
            case PARAMETER:
                return getIOptionNameDisplayFunction().apply(option);
            case DESCRIPTION:
                return option.description() == null ? "N/A" : option.description();
            case DEFAULT:
                return configManager.defaultTextForUsage(option, getIOptionNameDisplayFunction());
            default:
                throw new IllegalStateException(String.valueOf(column));
        }
    }

    private StringBuilder center(StringBuilder buf, String string, int width) {
        if (string == null) {
            string = "";
        }
        string = StringEscapeUtils.escapeHtml4(string);
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
        return pad(buf, string, width, ' ');
    }

    private StringBuilder pad(StringBuilder buf, String string, int width, char padChar) {
        if (string == null) {
            string = "";
        }
        string = StringEscapeUtils.escapeHtml4(string);
        buf.append(string);
        for (int i = width - string.length(); i > 0; i--) {
            buf.append(padChar);
        }
        return buf;
    }

}
