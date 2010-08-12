package edu.uci.ics.hyracks.control.common.application;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class ClassLoaderObjectInputStream extends ObjectInputStream {
    private ClassLoader classLoader;

    protected ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException,
            SecurityException {
        super(in);
        this.classLoader = classLoader;
    }

    protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
        return Class.forName(desc.getName(), false, classLoader);
    }
}