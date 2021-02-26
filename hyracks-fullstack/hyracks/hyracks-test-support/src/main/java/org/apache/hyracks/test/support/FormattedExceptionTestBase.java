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
package org.apache.hyracks.test.support;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IError;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class FormattedExceptionTestBase {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final Serializable[] FAKE_ARGS = new Serializable[] { "one", 2, 3.0f, 4.0d, (byte) 0x05 };
    private static final Serializable[] EMPTY_ARGS = new Serializable[0];
    private static final Class SERIALIZABLE_ARR_CLASS = EMPTY_ARGS.getClass();
    private static final org.apache.hyracks.api.exceptions.ErrorCode HYR_ERROR_CODE =
            random(org.apache.hyracks.api.exceptions.ErrorCode.values());
    private static final StackTraceElement[] STACK_TRACE = new Throwable().getStackTrace();
    private static final SourceLocation SOURCE_LOCATION = new SourceLocation(99, 9);
    private static final ClassLoader CLASSLOADER = FormattedExceptionTestBase.class.getClassLoader();
    private static final Field THROWABLE_DETAIL_MESSAGE = getDeclaredAccessibleField(Throwable.class, "detailMessage");
    private static final Field THROWABLE_CAUSE = getDeclaredAccessibleField(Throwable.class, "cause");
    private static final UnsupportedOperationException FAKE_THROWABLE = new UnsupportedOperationException();
    private static final int PUBLIC_STATIC = (Modifier.PUBLIC | Modifier.STATIC);
    private static Collection<Class<? extends IFormattedException>> exceptionClasses;
    protected static Set<Class<? extends IFormattedException>> roots;
    private static Set<Executable> publicContractOverrides = new HashSet<>();
    protected final Executable action;
    protected final Class<? extends IFormattedException> root;
    private static final Map<Pair<Class<? extends IFormattedException>, Object>, Field> rootFields = new HashMap<>();
    private static final Set<Class> visited = new HashSet<>();
    protected static Predicate<String> classSelector = className -> true;

    protected static Iterable<Object[]> defineParameters() throws ClassNotFoundException {
        initClasses();
        List<Object[]> tests = new ArrayList<>();
        for (Class<? extends IFormattedException> clazz : exceptionClasses) {
            Class<? extends IFormattedException> root = roots.stream().filter(c -> c.isAssignableFrom(clazz)).findAny()
                    .orElseThrow(IllegalStateException::new);
            final Constructor<?>[] declaredConstructors = clazz.getDeclaredConstructors();
            for (Constructor<?> ctor : declaredConstructors) {
                tests.add(new Object[] { clazz.getSimpleName() + ".<init>"
                        + Stream.of(ctor.getParameterTypes()).map(Class::getSimpleName).collect(Collectors.toList()),
                        ctor, root });
            }
            int methods = 0;
            for (Method m : clazz.getDeclaredMethods()) {
                if ((m.getModifiers() & PUBLIC_STATIC) == PUBLIC_STATIC) {
                    methods++;
                    tests.add(new Object[] { clazz.getSimpleName() + "." + m.getName()
                            + Stream.of(m.getParameterTypes()).map(Class::getSimpleName).collect(Collectors.toList()),
                            m, root });
                }
            }
            LOGGER.info("discovered {} ctors, {} methods for class {}", declaredConstructors.length, methods, clazz);
        }
        return tests;
    }

    protected static void addPublicContractOverride(Executable override) {
        publicContractOverrides.add(override);
    }

    public FormattedExceptionTestBase(String desc, Executable action, Class<? extends IFormattedException> root) {
        this.action = action;
        this.root = root;
    }

    @Test
    public void test() throws Exception {
        if (Modifier.isPublic(action.getModifiers())) {
            try {
                checkPublicContract();
            } catch (AssertionError e) {
                if (publicContractOverrides.contains(action)) {
                    LOGGER.info("ignoring public contract vioilation for override executable: " + action);
                } else {
                    throw e;
                }
            }
        }
        if (action.getName().equals("create") || action instanceof Constructor) {
            checkParameterPropagation(action);
        }
    }

    protected void checkPublicContract() {
        for (Class type : action.getParameterTypes()) {
            Assert.assertNotEquals("generic IError forbidden on public ctor or static method", type, IError.class);
        }
    }

    private void checkParameterPropagation(Executable factory) throws Exception {
        Object[] args = Stream.of(factory.getParameterTypes()).map(this::defaultValue).toArray(Object[]::new);
        factory.setAccessible(true);
        Field paramsField = rootParamsField();
        Object instance = factory instanceof Constructor ? ((Constructor) factory).newInstance(args)
                : ((Method) factory).invoke(null, args);
        Serializable[] params = (Serializable[]) paramsField.get(instance);
        IError error = null;
        for (Class type : factory.getParameterTypes()) {
            if (type.equals(paramsField.getType())) {
                Assert.assertArrayEquals(FAKE_ARGS, params);
            } else if (SourceLocation.class.isAssignableFrom(type)) {
                final Object value = rootSrcLocField().get(instance);
                Assert.assertEquals("source location is wrong, was: " + value, SOURCE_LOCATION, value);
            } else if (IError.class.isAssignableFrom(type)) {
                error = (IError) rootErrorField().get(instance);
                Assert.assertNotNull("error object", error);
            } else if (type.equals(Throwable.class)) {
                Assert.assertEquals(FAKE_THROWABLE, THROWABLE_CAUSE.get(instance));
            }
        }
        if (error != null) {
            Assert.assertEquals(error.component(), getRootField("component").get(instance));
            Assert.assertEquals(error.intValue(), getRootField("errorCode").get(instance));
            Assert.assertEquals(error.errorMessage(), THROWABLE_DETAIL_MESSAGE.get(instance));
        }
    }

    protected Field rootParamsField() {
        return getRootField(SERIALIZABLE_ARR_CLASS);
    }

    protected Field rootErrorField() {
        return getRootField(IError.class);
    }

    protected Field rootSrcLocField() {
        return getRootField(SourceLocation.class);
    }

    protected Field getRootField(Class<?> type) {
        return rootFields.computeIfAbsent(Pair.of(root, type),
                key -> Stream.of(root.getDeclaredFields()).filter(f -> f.getType().equals(type))
                        .peek(f -> f.setAccessible(true)).findAny().orElseThrow(IllegalStateException::new));
    }

    protected Field getRootField(String name) {
        return rootFields.computeIfAbsent(Pair.of(root, name), key -> getDeclaredAccessibleField(root, name));
    }

    protected static Field getDeclaredAccessibleField(Class clazz, String name) {
        try {
            Field field = clazz.getDeclaredField(name);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    protected Object defaultValue(Class type) {
        switch (type.getName()) {
            case "int":
                return 0;
            case "float":
                return 0.0f;
            case "double":
                return 0.0d;
            case "long":
                return 0L;
            case "boolean":
                return false;
            case "short":
                return (short) 0;
            case "byte":
                return (byte) 0;
            case "[Ljava.io.Serializable;":
                return FAKE_ARGS;
            case "java.lang.Throwable":
                return FAKE_THROWABLE;
            case "org.apache.hyracks.api.exceptions.IError":
            case "org.apache.hyracks.api.exceptions.ErrorCode":
                return HYR_ERROR_CODE;
            case "[Ljava.lang.StackTraceElement;":
                return STACK_TRACE;
            case "org.apache.hyracks.api.exceptions.SourceLocation":
                return SOURCE_LOCATION;
            case "org.apache.hyracks.api.exceptions.HyracksDataException":
                HyracksDataException hde = Mockito.mock(HyracksDataException.class);
                Mockito.when(hde.getError()).thenReturn(Optional.empty());
                return hde;
            default:
                if (type.isArray()) {
                    return Array.newInstance(type.getComponentType(), 0);
                } else if (type.isEnum()) {
                    return random(type.getEnumConstants());
                } else if (type.isAnonymousClass() || Modifier.isFinal(type.getModifiers())) {
                    if (visited.add(type)) {
                        LOGGER.info("defaulting to null for un-mockable class {}", type.getName());
                    }
                    return null;
                }
                if (visited.add(type)) {
                    LOGGER.info("defaulting to mock for unmapped class {}", type.getName());
                }
                return Mockito.mock(type);
        }
    }

    protected static <T> T random(T[] values) {
        return values[RandomUtils.nextInt(0, values.length)];
    }

    @SuppressWarnings("unchecked")
    private static void initClasses() throws ClassNotFoundException {
        LOGGER.info("discovering instances of IFormattedException");
        //noinspection unchecked
        final Class<IFormattedException> clazz =
                (Class) Class.forName(IFormattedException.class.getName(), false, CLASSLOADER);
        exceptionClasses =
                getInstanceClasses(clazz).sorted(Comparator.comparing(Class::getName)).collect(Collectors.toList());
        exceptionClasses.remove(clazz);
        LOGGER.info("found {} instances of IFormattedException: {}", exceptionClasses.size(), exceptionClasses);

        roots = exceptionClasses.stream().map(ex -> {
            while (IFormattedException.class.isAssignableFrom(ex.getSuperclass())) {
                ex = (Class<? extends IFormattedException>) ex.getSuperclass();
            }
            return ex;
        }).collect(Collectors.toSet());
        LOGGER.info("found {} roots: {}", roots.size(), roots);
        exceptionClasses.removeAll(roots);
    }

    @SuppressWarnings("unchecked")
    private static <T> Stream<Class<? extends T>> getInstanceClasses(Class<T> clazz) {
        return (Stream) getProductClasses().filter(name -> name.matches(".*(Exception|Error|Warning).*")).map(name -> {
            try {
                return Class.forName(name, false, CLASSLOADER);
            } catch (Throwable e) {
                LOGGER.warn("unable to open {} due to: {}", name, String.valueOf(e));
                return null;
            }
        }).filter(Objects::nonNull).filter(clazz::isAssignableFrom);
    }

    private static Stream<String> getProductClasses() {
        String[] cp = System.getProperty("java.class.path").split(File.pathSeparator);
        return Stream.of(cp).map(File::new).filter(File::isDirectory)
                .flatMap(FormattedExceptionTestBase::extractClassFiles).map(name -> name.replace("/", "."))
                .filter(classSelector).map(name -> name.replaceAll("\\.class$", "")).sorted();
    }

    private static Stream<? extends String> extractClassFiles(File dir) {
        final int beginIndex = dir.toString().length() + 1;
        return FileUtils.listFiles(dir, new String[] { "class" }, true).stream()
                .map(file -> file.getAbsolutePath().substring(beginIndex));
    }
}
