package edu.uci.ics.asterix.common.config;

public class AsterixCompilerProperties extends AbstractAsterixProperties {
    private static final String COMPILER_SORTMEMORY_KEY = "compiler.sortmemory";
    private static final long COMPILER_SORTMEMORY_DEFAULT = (512 << 20); // 512MB

    private static final String COMPILER_JOINMEMORY_KEY = "compiler.joinmemory";
    private static final long COMPILER_JOINMEMORY_DEFAULT = (512 << 20); // 512MB

    private static final String COMPILER_FRAMESIZE_KEY = "compiler.framesize";
    private static int COMPILER_FRAMESIZE_DEFAULT = (32 << 10); // 32KB

    public AsterixCompilerProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public long getSortMemorySize() {
        return accessor.getProperty(COMPILER_SORTMEMORY_KEY, COMPILER_SORTMEMORY_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public long getJoinMemorySize() {
        return accessor.getProperty(COMPILER_JOINMEMORY_KEY, COMPILER_JOINMEMORY_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public int getFrameSize() {
        return accessor.getProperty(COMPILER_FRAMESIZE_KEY, COMPILER_FRAMESIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }
}
