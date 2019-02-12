package nl.ease2pay.postalcode;

import nl.ease2pay.postalcode.config.Datasource;
import nl.ease2pay.postalcode.config.JdbcInfo;
import nl.ease2pay.postalcode.utils.PropertyHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class ProcessUpdateMO {

    private static Logger log = LoggerFactory.getLogger(ProcessUpdateMO.class);

    public static final String VERSION = "1.0.0";

    public static void main(String args[]){

        //Read dbconfig file
        String dbconfigfile = null;
        print_startup();

        String performUpdate="update";
        String performInsert="insert";
        String performDelete="delete";

        String streetTable="MO_POSTAL_CODE_STREET";
        String placeTable="MO_POSTAL_CODE_PLACE";
        String cityTable="MO_POSTAL_CODE_CITY";
        String houseTable="MO_POSTAL_CODE_HOUSENUMBER_XREF";


        if (args.length < 2) {
            log.error("Not all arguments are provided");
            print_usage();
            System.exit(0);
        } else {
            int i = 0;
            while (i < args.length) {
                if (args[i].equalsIgnoreCase("-c") && (i + 1) < args.length) {
                    dbconfigfile = args[i + 1];
                    i++;
                }
                i++;
            }
        }

        Properties props = PropertyHandling.readProperties(dbconfigfile);
        if (props == null) {
            log.error("Configfile not found");
            print_usage();
            System.exit(0);
        }
        //Establish DB connectivity
        //Retrieve Dashboard DSN
        Datasource pdsn = new Datasource(props, "postalcode");

        if (log.isDebugEnabled()) {
            pdsn.logDSN();
        }

        JdbcInfo dbinfo = new JdbcInfo(pdsn);

        Datasource mdsn = new Datasource(props, "myorder");

        if (log.isDebugEnabled()) {
            mdsn.logDSN();
        }

        JdbcInfo mdbinfo = new JdbcInfo(mdsn);

        try{
            //Select from mut_straat
            ResultSet rsstreetupdate= selectupdatemutstraat(dbinfo,performUpdate);
            updateMoPostalCodeStreet(mdbinfo,rsstreetupdate,streetTable);

            ResultSet rsstreetdelete= selectdeletemutstraat(dbinfo,performDelete);
            deleteMoPostalCodeStreet(mdbinfo,rsstreetdelete,streetTable);

            ResultSet rsstreetinsert= selectinsertmutstraat(dbinfo,performInsert);
            insertMoPostalCodeStreet(mdbinfo,rsstreetinsert,streetTable);


            // Update in MO_POSTAL_CODE_STREET


            //Select from mut_plaats
           ResultSet rsplaatsupdate = selectupdatemutplaats(dbinfo,performUpdate);

            ResultSet rsplaatsinsert = selectinsertmutplaats(dbinfo,performInsert);

            ResultSet rsplaatsdelete = selectdeletemutplaats(dbinfo,performDelete);

             // Update in MO_POSTAL_CODE_PLACE

            //Select from mut_pcreeks
            ResultSet rspcreeksupdate = selectupdatemutpcreeks(dbinfo,performUpdate);

            ResultSet rspcreeksdelete = selectdeletemutpcreeks(dbinfo,performDelete);

            ResultSet rspcreeksinsert = selectinsertmutpcreeks(dbinfo,performInsert);

            //Update in MO_POSTAL_CODE_HOUSENUMBER_XREF

            //Select from mut_gemeente
           ResultSet rsgemeenteupdate = selectupdatemutgemeente(dbinfo,performUpdate);

            ResultSet rsgemeenteinsert = selectinsertmutgemeente(dbinfo,performInsert);

            ResultSet rsgemeentedelete = selectdeletemutgemeente(dbinfo,performDelete);

            //Update in MO_POSTAL_CODE_CITY




        }
        catch (Exception sqle)
        {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage());
        }

        dbinfo.disconnectDB();


    }

    private static void insertMoPostalCodeStreet(JdbcInfo mdbinfo, ResultSet rsstreetinsert,String tableName) {
        try {
            while(rsstreetinsert.next()) {
                int streetid = 0;
                streetid = rsstreetinsert.getInt("STREET_ID");
                log.info("Street to be inserted:[{}]", streetid);
            }

        }
        catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void deleteMoPostalCodeStreet(JdbcInfo mdbinfo, ResultSet rsstreetdelete,String tableName) {

        try {
        while(rsstreetdelete.next()) {
            int streetid = 0;
            streetid = rsstreetdelete.getInt("STREET_ID");
            log.info("Street to be deleted:[{}]", streetid);
            }

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void updateMoPostalCodeStreet(JdbcInfo mdbinfo, ResultSet rsstreetupdate,String tableName) {
        try {
            while(rsstreetupdate.next()) {
                int streetid = 0;
                streetid = rsstreetupdate.getInt("STREET_ID");
                log.info("Street to be updated:[{}]", streetid);
            }

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static ResultSet selectdeletemutgemeente(JdbcInfo dbinfo, String performDelete) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_gemeente WHERE type='"+performDelete+"'";
            return dbinfo.doSelectQuery(query);
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]", sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectinsertmutgemeente(JdbcInfo dbinfo, String performInsert) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_gemeente WHERE type='"+performInsert+"'";
            ResultSet rs = dbinfo.doSelectQuery(query);
            return rs;
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectupdatemutgemeente(JdbcInfo dbinfo, String performUpdate) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_gemeente WHERE type='"+performUpdate+"'";
            ResultSet rs = dbinfo.doSelectQuery(query);
            return rs;
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectinsertmutpcreeks(JdbcInfo dbinfo, String performInsert) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_pcreeks WHERE type='"+performInsert+"'";
            ResultSet rs = dbinfo.doSelectQuery(query);
            return rs;
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectdeletemutpcreeks(JdbcInfo dbinfo, String performDelete) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_pcreeks WHERE type='"+performDelete+"'";
            return dbinfo.doSelectQuery(query);
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]", sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectupdatemutpcreeks(JdbcInfo dbinfo, String performUpdate) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_pcreeks WHERE type='"+performUpdate+"'";
            ResultSet rs = dbinfo.doSelectQuery(query);
            return rs;
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectdeletemutplaats(JdbcInfo dbinfo, String performDelete) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_plaats WHERE type='"+performDelete+"'";
            return dbinfo.doSelectQuery(query);
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]", sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectinsertmutplaats(JdbcInfo dbinfo, String performInsert) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_plaats WHERE type='"+performInsert+"'";
            ResultSet rs = dbinfo.doSelectQuery(query);
            return rs;
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectupdatemutplaats(JdbcInfo dbinfo, String performUpdate) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_plaats WHERE type='"+performUpdate+"'";
            ResultSet rs = dbinfo.doSelectQuery(query);
            return rs;
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        }
    return null;
    }


    private static ResultSet selectinsertmutstraat(JdbcInfo dbinfo, String performInsert) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_straat WHERE type='"+performInsert+"'";
            ResultSet rs = dbinfo.doSelectQuery(query);
            return rs;
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectdeletemutstraat(JdbcInfo dbinfo, String performDelete) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_straat WHERE type='"+performDelete+"'";
            return dbinfo.doSelectQuery(query);
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]", sqle.getMessage(), sqle.getErrorCode(), sqle.getSQLState());
        }
        return null;
    }

    private static ResultSet selectupdatemutstraat(JdbcInfo dbinfo, String performUpdate) {
        if (dbinfo.getConnection() == null) {
            log.error("Unable to open connection");
            return null;
        }
        try {
            String query = "SELECT * FROM mut_straat WHERE type='"+performUpdate+"'";
            ResultSet rs = dbinfo.doSelectQuery(query);
            return rs;
        } catch (SQLException sqle) {
            log.error("SQLException:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        }
        return null;
    }


    private static void print_startup()
    {
        log.info("ProcessUpdateMO Ease2pay POSTALCODE {}",VERSION);
        log.info("Copyright 2018-2019 (c) Ease2pay B.V.");
    }

    private static void print_usage()
    {
        log.info("Usage ProcessUpdateMO:");
        log.info("ProcessUpdateMO -c <configfile>");
    }



}
