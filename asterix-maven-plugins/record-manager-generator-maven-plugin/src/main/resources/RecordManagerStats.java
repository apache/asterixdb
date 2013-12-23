package @PACKAGE@;

public class RecordManagerStats {
    int arenas  = 0;
    int buffers = 0;
    int slots   = 0;
    int items   = 0;
    int size    = 0;
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ arenas : ").append(arenas);
        sb.append(", buffers : ").append(buffers);
        sb.append(", slots : ").append(slots);
        sb.append(", items : ").append(items);
        sb.append(", size : ").append(size).append(" }");
        return sb.toString();
    }
}
