package de.l3s.st_discovery.util.db;

import de.l3s.st_discovery.util.configuration.Configurable;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.configuration.ConfigurationParser;
import org.apache.commons.dbcp2.*;


import java.sql.*;

public class PostgreDB extends Configurable {

    private BasicDataSource source;

    public PostgreDB(Configuration config) {
        super(config);

        try {
            Class.forName("org.postgresql.Driver");

            source = new BasicDataSource();
            source.setDriverClassName("org.postgresql.Driver");
            source.setUrl("jdbc:postgresql://"+config.getStringOption("dbUrl")+"/"+config.getStringOption("dbName"));
            source.setUsername(config.getStringOption("dbUser"));
            source.setPassword(config.getStringOption("dbPassword"));
            source.setPoolPreparedStatements(true);

            if (config.hasOption("dbMaxConnections")) {
                source.setMaxTotal(config.getIntOption("dbMaxConnections"));
            } else {
                source.setMaxTotal(1);
            }

            if (config.hasOption("dbSchema")) {
                source.setDefaultSchema(config.getStringOption("dbSchema"));
            }

        } catch ( Exception e ) {
            e.printStackTrace();
            System.exit(2);
        }

    }

    /**
     * Returns a connection to the database
     * @return The connection
     */
    public Connection getConnection() {
        Connection con = null;
        try {
            con = source.getConnection();
            // use connection
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(2);
        }
        return con;
    }

    /**
     * Closes all connections.
     */
    public void close() {
        try {
            source.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void addConfigEntries(ConfigurationParser cp) {
        cp.addStringOption("dbu", "dbUrl", "Url of PostgreDB");
        cp.addStringOption("dbn", "dbName", "Name of PostgreDB");
        cp.addStringOption("dbun", "dbUser", "User of PostgreDB");
        cp.addStringOption("dbpw", "dbPassword", "Password for PostgreDB");
        cp.addStringOption("dbs", "dbSchema", "Default database schema");

    }

    public static void addConfigEntriesWitMaxCons(ConfigurationParser cp) {
        addConfigEntries(cp);
        cp.addStringOption("dbmc", "dbMaxConnections", "Maxmimum number of connections for PostgreDB");

    }
}
