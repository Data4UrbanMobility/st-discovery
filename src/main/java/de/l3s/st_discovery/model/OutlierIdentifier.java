package de.l3s.st_discovery.model;

import de.l3s.st_discovery.util.configuration.Configurable;
import de.l3s.st_discovery.util.configuration.Configuration;
import de.l3s.st_discovery.util.configuration.ConfigurationParser;
import de.l3s.st_discovery.util.db.PostgreDB;

import java.sql.*;

public class OutlierIdentifier extends Configurable {

    //config parameters
    private String inputTable, outlierTable, speedColumn, idColumn, timeColumn;

    private String statsTable;


    public static void registerConfigEntries(ConfigurationParser cp) {
        cp.addStringOption("it", "inputTable", "Name of the table holding the input data");
        cp.addStringOption("ot", "outlierTable", "Name of the table to store the outlier in");
        cp.addStringOption("sc", "speedColumn", "Name of the column that stores speed records");
        cp.addStringOption("ic", "idColumn", "Name of the column that stores record ids");
        cp.addStringOption("tc", "timeColumn", "Name of the column that stores record timestamps");
    }

    public OutlierIdentifier(Configuration config) {
        super(config);
        inputTable =  config.getStringOption("inputTable");
        outlierTable = config.getStringOption("outlierTable");
        speedColumn = config.getStringOption("speedColumn");
        idColumn = config.getStringOption("idColumn");
        timeColumn = config.getStringOption("timeColumn");

        //drop schema of input table
        String statsPrefix;
        if (inputTable.contains(".")) {
            statsPrefix = inputTable.substring(inputTable.indexOf(".")+1);
        } else {
            statsPrefix=inputTable;
        }

        statsTable = statsPrefix+"_stats";
    }


    public void run() throws SQLException {
        PostgreDB db = new PostgreDB(config);

        Connection con = db.getConnection();

        String dropOutlierQuery = "DROP TABLE IF EXISTS "+outlierTable+";";
        Statement dropOutlierStmt = con.createStatement();
        dropOutlierStmt.execute(dropOutlierQuery);
        dropOutlierStmt.close();

        String dropStatsQuery = "DROP TABLE IF EXISTS "+statsTable+";";
        Statement dropStatsStmt = con.createStatement();
        dropStatsStmt.execute(dropStatsQuery);
        dropStatsStmt.close();

        String statsQuery = "CREATE TABLE "+statsTable+" AS "+
                            "select "+idColumn+", extract('isodow' from "+timeColumn+") dow, "
                +""+timeColumn+"::time tod, " +
                "    max("+speedColumn+"), " +
                "    min("+speedColumn+"), " +
                "    avg("+speedColumn+"), " +
                "    public.median("+speedColumn+"::numeric), " +
                "    stddev("+speedColumn+"), " +
                "    percentile_cont(0.25) WITHIN GROUP (ORDER BY "+speedColumn+") q1, " +
                "    percentile_cont(0.75) WITHIN GROUP (ORDER BY "+speedColumn+") q3 " +
                "from "+inputTable+" " +
                "group by "+idColumn+", dow, tod;";

        System.out.println(statsQuery);

        Statement statsStmt = con.createStatement();
        statsStmt.execute(statsQuery);
        statsStmt.close();

        String indexQuery = "CREATE INDEX "+statsTable+"_"+idColumn+" on "+statsTable+"("+idColumn+")" ;
        Statement indexStmt = con.createStatement();
        indexStmt.execute(indexQuery);
        indexStmt.close();


        String outlierQuery = "create table "+outlierTable+" as " +
                "    select ip."+idColumn+", ip."+timeColumn+" , ip."+speedColumn+" from "+inputTable+" ip," +
                " "+statsTable+"  stats "+
                "   where extract('isodow' from ip."+timeColumn+") = stats.dow " +
                "   and ip."+timeColumn+"::time =  stats.tod " +
                "   and ip."+idColumn+" =  stats."+idColumn+" " +
                "   and ip."+speedColumn+" < stats.q1 - (1.5 * (stats.q3-stats.q1));";


        System.out.println(outlierQuery);
        Statement outlierStmt = con.createStatement();

        outlierStmt.execute(outlierQuery);
        outlierStmt.close();

        con.close();
        db.close();
    }
}
