/**
 * 
 */
package edu.uci.ics.asterix.aql.expression;

public class AdmSplitInfo {
    public Identifier nodeName;
    public String fileName;

    public AdmSplitInfo(Identifier nodeName, String fileName) {
        this.nodeName = nodeName;
        this.fileName = fileName;
    }

    @Override
    public String toString() {
        return nodeName.value + ":" + fileName;
    }
}