package edu.uci.ics.asterix.tools.external.data;

import java.util.Iterator;

import edu.uci.ics.asterix.tools.external.data.DataGenerator.InitializationInfo;
import edu.uci.ics.asterix.tools.external.data.DataGenerator.TweetMessage;

public class DataGeneratorDriver {

    public static void main(String[] args) {

        DataGenerator.initialize(new InitializationInfo());
        Iterator<TweetMessage> tweetIterator = DataGenerator.getTwitterMessageIterator();
        while (tweetIterator.hasNext()) {
            System.out.println(tweetIterator.next().toString());
        }
    }
}
