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
package org.apache.hyracks.control.common.config;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.collections4.map.CompositeMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.config.IConfigManager;
import org.apache.hyracks.api.config.IConfigurator;
import org.apache.hyracks.api.config.IOption;
import org.apache.hyracks.api.config.Section;
import org.apache.hyracks.api.config.SerializedOption;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.control.common.application.ConfigManagerApplicationConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ini4j.Ini;
import org.ini4j.Profile;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;

public class ConfigManager implements IConfigManager, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LogManager.getLogger();

    private HashSet<IOption> registeredOptions = new HashSet<>();
    @SuppressWarnings("squid:S1948") // HashMap is serializable, and therefore so is its synchronized map
    private Map<IOption, Object> definedMap = Collections.synchronizedMap(new HashMap<>());
    @SuppressWarnings("squid:S1948") // HashMap is serializable, and therefore so is its synchronized map
    private Map<IOption, Object> defaultMap = Collections.synchronizedMap(new HashMap<>());
    @SuppressWarnings("squid:S1948") // CompositeMap and his encapsulated maps are serializable, therefore so is this
    private Map<IOption, Object> configurationMap =
            Collections.synchronizedMap(new CompositeMap<>(definedMap, defaultMap, new NoOpMapMutator()));
    private EnumMap<Section, Map<String, IOption>> sectionMap = new EnumMap<>(Section.class);
    @SuppressWarnings("squid:S1948") // TreeMap is serializable, and therefore so is its synchronized map
    private Map<String, Map<IOption, Object>> nodeSpecificDefinedMap = Collections.synchronizedMap(new TreeMap<>());
    @SuppressWarnings("squid:S1948") // TreeMap is serializable, and therefore so is its synchronized map
    private Map<String, Map<IOption, Object>> nodeSpecificDefaultMap = Collections.synchronizedMap(new TreeMap<>());
    @SuppressWarnings("squid:S1948") // TreeMap is serializable, and therefore so is its synchronized map
    private Map<String, Map<IOption, Object>> nodeEffectiveMaps = Collections.synchronizedMap(new HashMap<>());
    private transient ArrayListValuedHashMap<IOption, IConfigSetter> optionSetters = new ArrayListValuedHashMap<>();
    private final String[] args;
    private ConfigManagerApplicationConfig appConfig = new ConfigManagerApplicationConfig(this);
    private Set<String> allSections = new HashSet<>();
    private transient Collection<Consumer<List<String>>> argListeners = new ArrayList<>();
    private transient Collection<IOption> iniPointerOptions = new ArrayList<>();
    private transient Collection<Section> cmdLineSections = new ArrayList<>();
    private transient OptionHandlerFilter usageFilter;
    private transient SortedMap<Integer, List<IConfigurator>> configurators = new TreeMap<>();
    private boolean configured;
    private String versionString = "version undefined";
    private transient Map<String, Set<Map.Entry<String, String>>> extensionOptions = new TreeMap<>();

    public ConfigManager() {
        this(null);
    }

    public ConfigManager(String[] args) {
        this.args = args;
        for (Section section : Section.values()) {
            allSections.add(section.sectionName());
        }
        addConfigurator(ConfiguratorMetric.PARSE_INI_POINTERS, this::extractIniPointersFromCommandLine);
        addConfigurator(ConfiguratorMetric.PARSE_INI, this::parseIni);
        addConfigurator(ConfiguratorMetric.PARSE_COMMAND_LINE, this::processCommandLine);
        addConfigurator(ConfiguratorMetric.APPLY_DEFAULTS, this::applyDefaults);
    }

    @Override
    public void addConfigurator(int metric, IConfigurator configurator) {
        configurators.computeIfAbsent(metric, metric1 -> new ArrayList<>()).add(configurator);
    }

    private void addConfigurator(ConfiguratorMetric metric, IConfigurator configurator) {
        addConfigurator(metric.metric(), configurator);
    }

    @Override
    public void addIniParamOptions(IOption... options) {
        Stream.of(options).forEach(iniPointerOptions::add);
    }

    @Override
    public void addCmdLineSections(Section... sections) {
        Stream.of(sections).forEach(cmdLineSections::add);
    }

    @Override
    public void setUsageFilter(OptionHandlerFilter usageFilter) {
        this.usageFilter = usageFilter;
    }

    @Override
    public void register(IOption... options) {
        for (IOption option : options) {
            if (option.section() == Section.VIRTUAL || registeredOptions.contains(option)) {
                continue;
            }
            if (configured) {
                throw new IllegalStateException("configuration already processed");
            }
            LOGGER.debug("registering option: " + option.toIniString());
            Map<String, IOption> optionMap = sectionMap.computeIfAbsent(option.section(), section -> new HashMap<>());
            IOption prev = optionMap.put(option.ini(), option);
            if (prev != null) {
                if (prev != option) {
                    throw new IllegalStateException("An option cannot be defined multiple times: "
                            + option.toIniString() + ": " + Arrays.asList(option.getClass(), prev.getClass()));
                }
            } else {
                registeredOptions.add(option);
                optionSetters.put(option, (node, value, isDefault) -> correctedMap(node, isDefault).put(option, value));
                if (LOGGER.isDebugEnabled()) {
                    optionSetters.put(option, (node, value, isDefault) -> LOGGER.debug("{} {} to {} for node {}",
                            isDefault ? "defaulting" : "setting", option.toIniString(), value, node));
                }
            }
        }
    }

    private Map<IOption, Object> correctedMap(String node, boolean isDefault) {
        if (node == null) {
            return isDefault ? defaultMap : definedMap;
        } else {
            synchronized (this) {
                ensureNode(node);
                return isDefault ? nodeSpecificDefaultMap.get(node) : nodeSpecificDefinedMap.get(node);
            }
        }
    }

    public synchronized void ensureNode(String nodeId) {
        LOGGER.trace("+ensureNode: {}", nodeId);
        Map<IOption, Object> nodeDefinedMap =
                nodeSpecificDefinedMap.computeIfAbsent(nodeId, this::createNodeSpecificMap);
        Map<IOption, Object> nodeDefaultMap =
                nodeSpecificDefaultMap.computeIfAbsent(nodeId, this::createNodeSpecificMap);
        nodeEffectiveMaps.computeIfAbsent(nodeId, id -> Collections
                .synchronizedMap(compositeFrom(Stream.of(nodeDefinedMap, nodeDefaultMap, definedMap))));
    }

    public synchronized void forgetNode(String nodeId) {
        LOGGER.trace("+forgetNode: {}", nodeId);
        nodeSpecificDefinedMap.remove(nodeId);
        nodeSpecificDefaultMap.remove(nodeId);
        nodeEffectiveMaps.remove(nodeId);
    }

    private Map<IOption, Object> createNodeSpecificMap(String nodeId) {
        LOGGER.trace("+createNodeSpecificMap: {}", nodeId);
        return Collections.synchronizedMap(new HashMap<>());
    }

    @Override
    @SafeVarargs
    public final void register(final Class<? extends IOption>... optionClasses) {
        for (Class<? extends IOption> optionClass : optionClasses) {
            register(optionClass.getEnumConstants());
        }
    }

    @Override
    public void setVersionString(String versionString) {
        this.versionString = versionString;
    }

    public IOption lookupOption(SerializedOption option) {
        return lookupOption(option.section().sectionName(), IOption.toIni(option.optionName()));
    }

    public IOption lookupOption(String section, String key) {
        Map<String, IOption> map = getSectionOptionMap(Section.parseSectionName(section));
        return map == null ? null : map.get(key);
    }

    public void processConfig() throws CmdLineException, IOException {
        if (!configured) {
            for (List<IConfigurator> configuratorList : configurators.values()) {
                for (IConfigurator configurator : configuratorList) {
                    configurator.run();
                }
            }
            configured = true;
        }
    }

    private void processCommandLine() throws CmdLineException {
        List<String> appArgs = processCommandLine(cmdLineSections, usageFilter, this::cmdLineSet);
        // now propagate the app args to the listeners...
        argListeners.forEach(l -> l.accept(appArgs));
    }

    private void extractIniPointersFromCommandLine() throws CmdLineException {
        Map<IOption, Object> cmdLineOptions = new HashMap<>();
        processCommandLine(cmdLineSections, usageFilter, cmdLineOptions::put);
        for (IOption option : iniPointerOptions) {
            if (cmdLineOptions.containsKey(option)) {
                set(option, cmdLineOptions.get(option));
            }
        }
    }

    private void cmdLineSet(IOption option, Object value) {
        invokeSetters(option, option.type().parse(String.valueOf(value)), null);
    }

    private synchronized void invokeSetters(IOption option, Object value, String nodeId) {
        optionSetters.get(option).forEach(setter -> setter.set(nodeId, value, false));
    }

    @SuppressWarnings({ "squid:S106", "squid:S1147" }) // use of System.err, System.exit()
    private List<String> processCommandLine(Collection<Section> sections, OptionHandlerFilter usageFilter,
            BiConsumer<IOption, Object> setAction) throws CmdLineException {
        final Args4jBean bean = new Args4jBean();
        CmdLineParser cmdLineParser = new CmdLineParser(bean);
        final List<String> appArgs = new ArrayList<>();
        List<IOption> commandLineOptions = new ArrayList<>();
        for (Map.Entry<Section, Map<String, IOption>> sectionMapEntry : sectionMap.entrySet()) {
            if (!sections.contains(sectionMapEntry.getKey())) {
                continue;
            }
            for (IOption option : sectionMapEntry.getValue().values()) {
                if (option.section() != Section.VIRTUAL) {
                    commandLineOptions.add(option);
                }
            }
        }
        commandLineOptions.sort(Comparator.comparing(IOption::cmdline));

        commandLineOptions.forEach(option -> cmdLineParser.addOption(new Args4jSetter(option, setAction, false),
                new Args4jOption(option, this, option.type().targetType())));

        if (!argListeners.isEmpty()) {
            cmdLineParser.addArgument(new Args4jSetter(o -> appArgs.add(String.valueOf(o)), true, String.class),
                    new Args4jArgument());
        }
        LOGGER.debug("parsing cmdline: " + Arrays.toString(args));
        if (args == null || args.length == 0) {
            LOGGER.info("no command line args supplied");
            return appArgs;
        }
        try {
            cmdLineParser.parseArgument(args);
        } catch (CmdLineException e) {
            if (!bean.help) {
                ConfigUtils.printUsage(e, usageFilter, System.err);
                throw e;
            } else {
                LOGGER.log(Level.DEBUG, "Ignoring parse exception due to -help", e);
            }
        }
        if (bean.help) {
            ConfigUtils.printUsage(cmdLineParser, usageFilter, System.err);
            System.exit(0);
        } else if (bean.version) {
            System.err.println(versionString);
            System.exit(0);
        }
        return appArgs;
    }

    private void parseIni() throws IOException {
        Ini ini = null;
        for (IOption option : iniPointerOptions) {
            Object pointer = get(option);
            if (pointer instanceof String) {
                ini = ConfigUtils.loadINIFile((String) pointer);
            } else if (pointer instanceof URL) {
                ini = ConfigUtils.loadINIFile((URL) pointer);
            } else if (pointer != null) {
                throw new IllegalArgumentException("config file pointer options must be of type String (for file) or "
                        + "URL, instead of " + option.type().targetType());
            }
        }
        if (ini == null) {
            LOGGER.info("no INI file specified; skipping parsing");
            return;
        }
        LOGGER.info("parsing INI file: " + ini);
        for (Profile.Section section : ini.values()) {
            allSections.add(section.getName());
            final Section rootSection = Section
                    .parseSectionName(section.getParent() == null ? section.getName() : section.getParent().getName());
            String node;
            if (rootSection == Section.EXTENSION) {
                extensionOptions.put(section.getName(), section.entrySet());
                continue;
            } else if (rootSection == Section.NC) {
                node = section.getName().equals(section.getSimpleName()) ? null : section.getSimpleName();
            } else if (Section.parseSectionName(section.getName()) != null) {
                node = null;
            } else {
                throw new HyracksException("Unknown section in ini: " + section.getName());
            }
            Map<String, IOption> optionMap = getSectionOptionMap(rootSection);
            for (Map.Entry<String, String> iniOption : section.entrySet()) {
                String name = iniOption.getKey();
                final IOption option = optionMap == null ? null : optionMap.get(name);
                if (option == null) {
                    handleUnknownOption(section, name);
                    return;
                }
                final String value = iniOption.getValue();
                LOGGER.debug("setting " + option.toIniString() + " to " + value);
                final Object parsed = option.type().parse(value);
                invokeSetters(option, parsed, node);
            }
        }
    }

    private void handleUnknownOption(Profile.Section section, String name) throws HyracksException {
        Set<String> matches = new HashSet<>();
        for (IOption registeredOption : registeredOptions) {
            if (registeredOption.ini().equals(name)) {
                matches.add(registeredOption.section().sectionName());
            }
        }
        if (!matches.isEmpty()) {
            throw new HyracksException(
                    "Section mismatch for [" + section.getName() + "] " + name + ", expected section(s) " + matches);
        } else {
            throw new HyracksException("Unknown option in ini: [" + section.getName() + "] " + name);
        }
    }

    private void applyDefaults() {
        LOGGER.debug("applying defaults");
        sectionMap.forEach((key, value) -> {
            value.values().forEach(
                    option -> getNodeNames().forEach(node -> getOrDefault(getNodeEffectiveMap(node), option, node)));
            for (Map.Entry<String, Map<IOption, Object>> nodeMap : nodeSpecificDefinedMap.entrySet()) {
                value.values()
                        .forEach(option -> getOrDefault(
                                new CompositeMap<>(nodeMap.getValue(), definedMap, new NoOpMapMutator()), option,
                                nodeMap.getKey()));
            }
        });
    }

    private synchronized Object getOrDefault(Map<IOption, Object> map, IOption option, String nodeId) {
        if (map.containsKey(option)) {
            return map.get(option);
        } else {
            Object value = resolveDefault(option, new ConfigManagerApplicationConfig(this) {
                private static final long serialVersionUID = -5505664489371709335L;

                @Override
                public Object getStatic(IOption option) {
                    return getOrDefault(map, option, nodeId);
                }
            });
            if (value != null && optionSetters != null) {
                optionSetters.get(option).forEach(setter -> setter.set(nodeId, value, true));
            }
            return value;
        }
    }

    public Object resolveDefault(IOption option, IApplicationConfig applicationConfig) {
        final Object value = option.defaultValue();
        if (value instanceof IOption) {
            return applicationConfig.get((IOption) value);
        } else if (value instanceof Supplier) {
            //noinspection unchecked
            return ((Supplier<?>) value).get();
        } else if (value instanceof Function) {
            //noinspection unchecked
            return ((Function<IApplicationConfig, ?>) value).apply(applicationConfig);
        } else {
            return value;
        }
    }

    @Override
    public Set<Section> getSections(Predicate<Section> predicate) {
        return Arrays.stream(Section.values()).filter(predicate).collect(Collectors.toSet());
    }

    @Override
    public Set<Section> getSections() {
        return getSections(section -> true);
    }

    public Set<String> getSectionNames() {
        return Collections.unmodifiableSet(allSections);
    }

    public Set<String> getOptionNames(String sectionName) {
        Set<String> optionNames = new HashSet<>();
        Section section = Section.parseSectionName(sectionName);
        for (IOption option : getSectionOptionMap(section).values()) {
            optionNames.add(option.ini());
        }
        return optionNames;
    }

    @Override
    public Set<IOption> getOptions(Section section) {
        return new HashSet<>(getSectionOptionMap(section).values());
    }

    private Map<String, IOption> getSectionOptionMap(Section section) {
        final Map<String, IOption> map = sectionMap.get(section);
        return map != null ? map : Collections.emptyMap();
    }

    public List<String> getNodeNames() {
        return Collections.unmodifiableList(new ArrayList<>(nodeSpecificDefinedMap.keySet()));
    }

    public synchronized IApplicationConfig getNodeEffectiveConfig(String nodeId) {
        ensureNode(nodeId);
        final Map<IOption, Object> nodeMap = nodeSpecificDefaultMap.get(nodeId);
        Map<IOption, Object> nodeEffectiveMap = getNodeEffectiveMap(nodeId);
        return new ConfigManagerApplicationConfig(this) {
            private static final long serialVersionUID = 3166949269001016392L;

            @Override
            public Object getStatic(IOption option) {
                if (!nodeEffectiveMap.containsKey(option)) {
                    // we need to calculate the default within the context of the node specific map...
                    nodeMap.put(option, getOrDefault(nodeEffectiveMap, option, nodeId));
                }
                return nodeEffectiveMap.get(option);
            }
        };
    }

    private synchronized Map<IOption, Object> getNodeEffectiveMap(String nodeId) {
        ensureNode(nodeId);
        return nodeEffectiveMaps.get(nodeId);
    }

    private synchronized CompositeMap<IOption, Object> compositeFrom(Stream<Map<IOption, Object>> stream) {
        List<Map<IOption, Object>> list = stream.collect(Collectors.toList());
        CompositeMap<IOption, Object> map = new CompositeMap<>();
        map.setMutator(new NoOpMapMutator());
        for (ListIterator<Map<IOption, Object>> iter = list.listIterator(list.size()); iter.hasPrevious();) {
            map.addComposited(iter.previous());
        }
        return map;
    }

    public synchronized Ini toIni(boolean includeDefaults) {
        Ini ini = new Ini();
        (includeDefaults ? configurationMap : definedMap).forEach((option, value) -> {
            if (value != null) {
                ini.add(option.section().sectionName(), option.ini(), option.type().serializeToIni(value));
            }
        });
        for (String key : getNodeNames()) {
            String section = Section.NC.sectionName() + "/" + key;
            ensureNode(key);
            Map<IOption, Object> nodeValueMap =
                    includeDefaults ? getNodeEffectiveMap(key) : nodeSpecificDefinedMap.get(key);
            for (Map.Entry<IOption, Object> entry : nodeValueMap.entrySet()) {
                if (entry.getValue() != null) {
                    final IOption option = entry.getKey();
                    ini.add(section, option.ini(), option.type().serializeToIni(entry.getValue()));
                }
            }
        }
        extensionOptions.forEach((extension, options) -> options
                .forEach(option -> ini.add(extension, option.getKey(), option.getValue())));
        return ini;
    }

    public void set(IOption option, Object value) {
        set(null, option, value);
    }

    public synchronized void set(String nodeId, IOption option, Object value) {
        invokeSetters(option, copyIfArray(value), nodeId);
    }

    private Object copyIfArray(Object orig) {
        if (orig == null || !orig.getClass().isArray()) {
            return orig;
        }
        int arrayLength = Array.getLength(orig);
        Object copy = Array.newInstance(orig.getClass().getComponentType(), arrayLength);
        IntStream.range(0, arrayLength).forEach(i -> Array.set(copy, i, Array.get(orig, i)));
        return copy;
    }

    public Object get(IOption option) {
        if (!registeredOptions.contains(option)) {
            throw new IllegalStateException("Option not registered with ConfigManager: " + option.toIniString() + "("
                    + option.getClass() + "." + option + ")");
        } else if (option.section() == Section.NC) {
            LOGGER.warn("NC option " + option.toIniString() + " being accessed outside of NC-scoped configuration.");
        }
        return getOrDefault(configurationMap, option, null);
    }

    public Set<IOption> getOptions() {
        return Collections.unmodifiableSet(registeredOptions);
    }

    @Override
    public IApplicationConfig getAppConfig() {
        return appConfig;
    }

    public void registerArgsListener(Consumer<List<String>> argListener) {
        argListeners.add(argListener);
    }

    String getUsage(IOption option) {
        final String description = option.description();
        StringBuilder usage = new StringBuilder();
        if (description != null && !"".equals(description)) {
            usage.append(description).append(" ");
        } else {
            LOGGER.warn("missing description for option: {}.{}", option.getClass().getSimpleName(), option.name());
        }
        usage.append("(default: ");
        usage.append(defaultTextForUsage(option, IOption::cmdline));
        usage.append(")");
        return usage.toString();
    }

    public String defaultTextForUsage(IOption option, Function<IOption, String> optionPrinter) {
        StringBuilder buf = new StringBuilder();
        String override = option.usageDefaultOverride(appConfig, optionPrinter);
        if (override != null) {
            buf.append(override);
        } else {
            final Object value = option.defaultValue();
            if (value instanceof IOption) {
                buf.append("same as ").append(optionPrinter.apply((IOption) value));
            } else if (value instanceof Function) {
                // TODO(mblow): defer usage calculation to enable evaluation of function
                buf.append("<function>");
            } else if (value == null) {
                buf.append("<undefined>");
            } else {
                buf.append(option.type().serializeToHumanReadable(resolveDefault(option, appConfig)));
            }
        }
        return buf.toString();
    }

    private static class NoOpMapMutator implements CompositeMap.MapMutator<IOption, Object> {
        private static final long serialVersionUID = -7239155627408457440L;

        @Override
        public Object put(CompositeMap<IOption, Object> compositeMap, Map<IOption, Object>[] maps, IOption iOption,
                Object o) {
            throw new UnsupportedOperationException("mutations are not allowed");
        }

        @Override
        public void putAll(CompositeMap<IOption, Object> compositeMap, Map<IOption, Object>[] maps,
                Map<? extends IOption, ?> map) {
            throw new UnsupportedOperationException("mutations are not allowed");
        }

        @Override
        public void resolveCollision(CompositeMap<IOption, Object> composite, Map<IOption, Object> existing,
                Map<IOption, Object> added, Collection<IOption> intersect) {
            LOGGER.debug("resolveCollision: {}, {}, {}, {}", composite, existing, added, intersect);
            // no-op
        }
    }

    private static class Args4jBean {
        @Option(name = "-help", help = true)
        boolean help;

        @Option(name = "-version", help = true)
        boolean version;
    }
}
