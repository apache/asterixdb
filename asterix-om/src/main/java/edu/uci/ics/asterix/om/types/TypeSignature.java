package edu.uci.ics.asterix.om.types;

import java.io.Serializable;

public class TypeSignature implements Serializable {

    private final String dataverse;
    private final String name;
    private final String alias;

    public TypeSignature(String namespace, String name) {
        this.dataverse = namespace;
        this.name = name;
        this.alias = dataverse + "@" + name;
    }

    public boolean equals(Object o) {
        if (!(o instanceof TypeSignature)) {
            return false;
        } else {
            TypeSignature f = ((TypeSignature) o);
            return dataverse.equals(f.getNamespace()) && name.equals(f.getName());
        }
    }

    public String toString() {
        return alias;
    }

    public int hashCode() {
        return alias.hashCode();
    }

    public String getNamespace() {
        return dataverse;
    }

    public String getName() {
        return name;
    }

}
