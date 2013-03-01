package edu.uci.ics.hyracks.algebricks.examples.piglet.ast;

public class DumpNode extends RelationNode {
    private final String file;
    private final String alias;

    public DumpNode(String file, String alias) {
        this.file = file;
        this.alias = alias;
    }

    @Override
    public Tag getTag() {
        return Tag.DUMP;
    }

    public String getFile() {
        return file;
    }

    public String getAlias() {
        return alias;
    }
}