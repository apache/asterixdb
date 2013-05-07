package edu.uci.ics.asterix.common.config;

public class AsterixCompilerProperties extends AbstractAsterixProperties {
    private static final String COMPILER_SORTMEMORY_KEY = "compiler.sortmemory";
    private static final int COMPILER_SORTMEMORY_DEFAULT = (512 << 20); // 512MB

    private static final String COMPILER_JOINMEMORY_KEY = "compiler.joinmemory";
    private static final int COMPILER_JOINMEMORY_DEFAULT = (512 << 20); // 512MB

    private static final String COMPILER_FRAMESIZE_KEY = "compiler.framesize";
    private static int COMPILER_FRAMESIZE_DEFAULT = (32 << 10); // 32KB

    public AsterixCompilerProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public int getSortMemorySize() {
        return accessor.getInt(COMPILER_SORTMEMORY_KEY, COMPILER_SORTMEMORY_DEFAULT);
    }

    public int getJoinMemorySize() {
        return accessor.getInt(COMPILER_JOINMEMORY_KEY, COMPILER_JOINMEMORY_DEFAULT);
    }

    public int getFrameSize() {
        return accessor.getInt(COMPILER_FRAMESIZE_KEY, COMPILER_FRAMESIZE_DEFAULT);
    }
}
