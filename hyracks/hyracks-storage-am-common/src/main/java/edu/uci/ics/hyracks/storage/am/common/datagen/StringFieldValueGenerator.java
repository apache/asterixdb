package edu.uci.ics.hyracks.storage.am.common.datagen;

import java.util.Random;

public class StringFieldValueGenerator implements IFieldValueGenerator<String> {
    private int maxLen;
    private final Random rnd;
    
    public StringFieldValueGenerator(int maxLen, Random rnd) {
        this.maxLen = maxLen;
        this.rnd = rnd;
    }

    public void setMaxLength(int maxLen) {
        this.maxLen = maxLen;
    }
    
    @Override
    public String next() {
        String s = Long.toHexString(Double.doubleToLongBits(rnd.nextDouble()));
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < s.length() && i < maxLen; i++) {
            strBuilder.append(s.charAt(Math.abs(rnd.nextInt()) % s.length()));
        }
        return strBuilder.toString();
    }
}
