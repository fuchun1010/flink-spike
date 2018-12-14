package com.tank.utils;

import lombok.var;
import org.apache.commons.cli.*;

/**
 *
 */
public class ParameterTool {

  public CommandLine fetchInputedCommand(final String[] args) {
    final Options options = new Options();
    final Option zkOpt = new Option("zk", true, "zookeeper connect ip:port");
    zkOpt.setRequired(true);

    final Option parallelismOpt = new Option("p", true, "parallelism");
    parallelismOpt.setRequired(true);

    options.addOption(zkOpt);
    options.addOption(parallelismOpt);

    var parser = new DefaultParser();

    try {
      return parser.parse(options, args);
    } catch (ParseException e) {
      e.printStackTrace();
      return null;
    }
  }

  
}
