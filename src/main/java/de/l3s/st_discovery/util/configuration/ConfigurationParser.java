package de.l3s.st_discovery.util.configuration;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public class ConfigurationParser {

    private CommandLineParser parser;
    private Options options;
    private List<Configuration> configs;


    public ConfigurationParser() {
        parser = new DefaultParser();
        options = new Options();

        addFlag("h", "help", "Show this message");
        addStringOption("c", "config", "Path to configuration file");
    }

    public void addFlag(String argName, String longOpt, String help) {
        Option opt = Option.builder(argName)
                .argName(longOpt)
                .longOpt(longOpt)
                .desc(help)
                .hasArg(false)
                .build();
        options.addOption(opt);
    }

    public void addStringOption(String argName, String longOpt, String help) {
        addTypeOption(argName, longOpt, help, String.class);
    }

    public void addIntOption(String argName, String longOpt, String help) {
        addTypeOption(argName, longOpt, help, Integer.class);
    }

    public void addDoubleOption(String argName, String longOpt, String help) {
        addTypeOption(argName, longOpt, help, Double.class);
    }

    public void addBooleanOption(String argName, String longOpt, String help) {
        addTypeOption(argName, longOpt, help, Boolean.class);
    }

    private void addTypeOption(String argName, String longOpt, String help, Class type) {
        Option opt = Option.builder(argName)
                .argName(longOpt)
                .longOpt(longOpt)
                .desc(help)
                .hasArg(true)
                .type(type)
                .build();
        options.addOption(opt);

    }


    public void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(" ", options);
    }

    public void parse(String[] args) {
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.err.println("Error parsing command line arguments");
            printHelp();
            System.exit(0);
        }

        if (args.length == 0 || commandLine.hasOption("h")) {
            printHelp();
            System.exit(0);
        }

        CommandLine configFile = null;
        try {
            if (commandLine.hasOption("c")) {
                String[] configArgs = new String[0];
                configArgs = parseConfigFile(commandLine.getOptionValue("c"));
                configFile = parser.parse(options, configArgs);
            } else {
                configFile = parser.parse(options, new String[0]);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Can not read config file");
            System.exit(1);
        } catch (ParseException e) {
            e.printStackTrace();
            System.err.println("Error parsing config file");
            printHelp();
            System.exit(1);
        }

        splitConfigs(commandLine, configFile);
    }

    private void splitConfigs(CommandLine commandLine, CommandLine config) {
        Set<String> mutliCL = getMultiValuedArguments(commandLine);
        Set<String> mutliConfig = getMultiValuedArguments(config);

        //ignore options in config that are set in the command line
        for (String opt: mutliConfig) {
            if (commandLine.hasOption(opt)) {
                mutliConfig.remove(opt);
            }
        }

        configs = new ArrayList<>();
        try {
            recursiveValueSplit(new ArrayList<String>(mutliCL),
                    0,
                    new ArrayList<>(mutliConfig),
                    0,
                    commandLine,
                    config,
                    configs);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void recursiveValueSplit(List<String> clArgs,
                                     int clIndex,
                                     List<String> configArgs,
                                     int configIndex,
                                     CommandLine currentCl,
                                     CommandLine currentConfig,
                                     List<Configuration> result) throws ParseException {

        if (clIndex == clArgs.size() && (configIndex == configArgs.size())) {
            result.add(new Configuration(currentCl, currentConfig));
            return;
        }

        if(clIndex < clArgs.size()) {
            List<CommandLine> next = splitSingleOption(clArgs.get(clIndex), currentCl);
            for (CommandLine n: next) {
                recursiveValueSplit(clArgs, clIndex+1, configArgs, configIndex, n, currentConfig, result);
            }
        } else if (configIndex < configArgs.size()) {
            List<CommandLine> next = splitSingleOption(configArgs.get(configIndex), currentConfig);
            for (CommandLine n: next) {
                recursiveValueSplit(clArgs, clIndex, configArgs, configIndex+1, currentCl, n, result);
            }
        }

    }

    private List<CommandLine> splitSingleOption(String opt, CommandLine cl) throws ParseException {
        List<CommandLine> result = new ArrayList<>();

        String[] vals = cl.getOptionValue(opt).split(",");
        DefaultParser tempParser = new DefaultParser();

        for (String v: vals) {
            List<String> args = new ArrayList<>();
            //add old aruments
            for (Option current: cl.getOptions()) {
                String name = current.getArgName();

                //don't add the splitted argument
                if (name.equals(opt)) continue;

                String value = current.getValue(name);
                args.add("--"+name);
                if (value !=null) args.add(value);
            }
            //the splitted argument is added here
            args.add("--"+opt);
            args.add(v);

            CommandLine r = tempParser.parse(options, args.toArray(new String[0]));
            result.add(r);
        }
        return result;
    }

    private Set<String> getMultiValuedArguments(CommandLine cli) {
        Set<String> result = new HashSet<>();

        for(Option opt: cli.getOptions()) {
           String optVal = opt.getValue();
           if (optVal==null) continue;
           if (optVal.contains(",")) result.add(opt.getArgName());
        }
        return result;
    }

    private String[] parseConfigFile(String path) throws IOException {
        List<String> result = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(path))) {
            stream.forEach(l -> {
                String[] parts = l.split("=");
                for (int i = 0; i < parts.length; ++i) {

                    if (i == 0 && options.hasShortOption(parts[i])) {
                        result.add("-" + parts[i]);
                    } else if (i == 0 && options.hasLongOption(parts[i])) {
                        result.add("--" + parts[i]);
                    } else {
                        result.add(parts[i]);
                    }
                }
            });
        }
        return result.toArray(new String[0]);
    }

    public List<Configuration> getConfigs() {
        return configs;
    }
}
