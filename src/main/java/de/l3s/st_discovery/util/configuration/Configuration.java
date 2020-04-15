package de.l3s.st_discovery.util.configuration;

import org.apache.commons.cli.CommandLine;

import java.util.HashMap;
import java.util.Map;


public class Configuration {

    private CommandLine commandLine, configFile;
    private Map<String, String> variables;


    public Configuration(CommandLine commandLine, CommandLine configFile) {
        this.commandLine = commandLine;
        this.configFile = configFile;
        this.variables = new HashMap<>();
    }

    public String getStringVariable(String name) {
        return variables.get(name);
    }

    public double getDoubleVariable(String name) {
        return Double.parseDouble(variables.get(name));
    }

    public int getIntVariable(String name) {
        return Integer.parseInt(variables.get(name));
    }

    public boolean getBoolVariable(String name) {
        return Boolean.parseBoolean(variables.get(name));
    }

    public void setVariable(String name, Object value) {
        variables.put(name, value.toString());
    }

    public String getStringOption(String name) {
        return (String) getOption(name);
    }

    public String getStringOption(String name, String defaultValue) {
        return (String) getOption(name, defaultValue);
    }

    public int getIntOption(String name) {
        return Integer.parseInt(getOption(name));
    }

    public int getIntOption(String name, int defaultValue) {
        return Integer.parseInt(getOption(name, defaultValue));
    }

    public double getDoubleOption(String name) {
        return Double.parseDouble(getOption(name));
    }

    public double getDoubleOption(String name, double defaultValue) {
        return Double.parseDouble(getOption(name, defaultValue));
    }

    public boolean getBooleanOption(String name) {
        return Boolean.parseBoolean(getOption(name));
    }

    public boolean getBooleanOption(String name, boolean defaultValue) {
        return Boolean.parseBoolean(getOption(name, defaultValue));
    }


    public boolean hasOption(String name) {
        return ((commandLine.getOptionValue(name) != null) || (configFile.getOptionValue(name) != null));
    }

    public boolean getFlag(String name) {
        return (configFile.hasOption(name) || commandLine.hasOption(name));
    }

    private String getOption(String name) {
        String result=null;
            result = commandLine.getOptionValue(name);
            if (result == null) {
                result = configFile.getOptionValue(name);
            }

            if (result==null) {
                System.err.println("Can not find parsed option \""+name+"\".");
                System.exit(1);
            }

        return result;

    }

    private String getOption(String name, Object defaultValue) {
        String result=null;
        result = commandLine.getOptionValue(name);
        if (result == null) {
            result = configFile.getOptionValue(name, defaultValue.toString());
        }

        return result;
    }

}
