package edu.uci.ics.asterix.aql.expression;

public final class VarIdentifier extends Identifier {
    private int id;

    public VarIdentifier() {
        super();
    }

    public VarIdentifier(String value) {
        super();
        this.value = value;
    }

    public VarIdentifier(String value, int id) {
        super();
        this.value = value;
        this.id = id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public VarIdentifier clone() {
        VarIdentifier vi = new VarIdentifier(this.value);
        vi.setId(this.id);
        return vi;
    }
}
