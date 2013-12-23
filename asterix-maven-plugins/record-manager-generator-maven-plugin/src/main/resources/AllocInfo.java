package @PACKAGE@;

import java.io.PrintWriter;
import java.io.StringWriter;

public class AllocInfo {
    String alloc;
    String free;
    
    void alloc() {
        alloc = getStackTrace();
    }
    
    void free() {
        free = getStackTrace();
    }

    private String getStackTrace() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        new Exception().printStackTrace(pw);
        pw.close();
        String res = sw.toString();
        // remove first 3 lines
        int nlPos = 0;
        for (int i = 0; i < 3; ++i) {
            nlPos = res.indexOf('\n', nlPos) + 1; 
        }
        return res.substring(nlPos);
    }
    
    public String toString() {
        return "allocation stack:\n" + alloc + "\nfree stack\n" + free;
    }
}
