package edu.uci.ics.asterix.installer.command;

import org.kohsuke.args4j.Option;

public class AbstractCommandConfig implements CommandConfig {

    @Option(name = "-help", required = false, usage = "Help")
    public boolean help = false;

    @Override
    public boolean helpMode() {
        return help;
    }

}
