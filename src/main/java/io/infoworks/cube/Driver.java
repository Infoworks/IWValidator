package io.infoworks.cube;

import org.apache.commons.cli.*;

/**
 * Created by abhishek on 06/08/16.
 */
public class Driver {
  // Options
  private static final String KYLIN_HOST = "kylinHost";
  private static final String KYLIN_PORT = "kylinPort";
  private static final String KYLIN_USERNAME = "kylinUsername";
  private static final String KYLIN_PASSWORD =  "kylinPassword";
  private static final String KYLIN_PROJECTNAME = "kylinProjectName";

  private static String kylinHost;
  private static String kylinPort;
  private static String kylinUsername;
  private static String kylinPassword;
  private static String kylinProjectName;

  private static Options createOptions() {
    final Options options = new Options();
    options.addOption(Option.builder().type(String.class).longOpt(KYLIN_HOST).hasArg().required().build());
    options.addOption(Option.builder().type(String.class).longOpt(KYLIN_PORT).hasArg().required().build());
    options.addOption(Option.builder().type(String.class).longOpt(KYLIN_USERNAME).hasArg().required().build());
    options.addOption(Option.builder().type(String.class).longOpt(KYLIN_PASSWORD).hasArg().required().build());
    options.addOption(Option.builder().type(String.class).longOpt(KYLIN_PROJECTNAME).hasArg().required().build());
    return options;
  }

  private static CommandLine getCommandLine(final Options options, final String[] args)
    throws ParseException {
    final CommandLineParser parser = new GnuParser();
    CommandLine line = null;
    line = parser.parse(options, args);
    return line;
  }

  private static void initialize(CommandLine line) {
      kylinHost = line.getOptionValue(KYLIN_HOST);
      kylinPort = line.getOptionValue(KYLIN_PORT);
      kylinUsername = line.getOptionValue(KYLIN_USERNAME);
      kylinPassword = line.getOptionValue(KYLIN_PASSWORD);
      kylinProjectName = line.getOptionValue(KYLIN_PROJECTNAME);
  }

  public static void main(String[] args) throws Exception{
    final Options options = createOptions();
    initialize(getCommandLine(options, args));
    QueryExecutor executor = new QueryExecutor(kylinHost, kylinPort, kylinUsername, kylinPassword, kylinProjectName);
    executor.executeQueriesFromFile();
  }
}
