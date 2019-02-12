package nl.ease2pay.postalcode.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class JdbcInfo {
	
	/** Logger */
	private static Logger log = LoggerFactory.getLogger(JdbcInfo.class);
	
	private String driver;
	private String url;
    private String user= "";
    private String passwd="";
    private Connection connection = null;
   
    /**
     * Constructor JdbcInfo
     * @param driver The driver
     * @param url The connection url
     * @param user The user to connect
     * @param password The password for the user
     */
    public JdbcInfo(String driver, String url, String user, String password) {
    	setDriver(driver);
    	setUrl(url);
    	setUser(user);
    	setPassword(password);
    	this.connection = null;
    }
        
    /**
     * Constructor JdbcInfo
     * @param jdsn The JdbcDSN
     */
    public JdbcInfo(Datasource jdsn) {
    	if (jdsn != null) {
    		setDriver(jdsn.getDriver());
    		setUrl(jdsn.getUrl());
    		setUser(jdsn.getUser());
    		setPassword(jdsn.getPassword());
    	}
    	this.connection = null;    	
    }
    
    /**
     * This method set the drivertype 
     * @param driver The drivertype to set.
     */
    public void setDriver(String driver) {
    	this.driver = driver;
    }

    /**
     * Returns the driver
     * @return
     */
    public String getDriver() {
    	return this.driver;
    }
    
    /**
     * This method set the connection ulr
     * @param url The connection url.
     */
    public void setUrl(String url) {
    	this.url = url;
    }

    /**
     * Returns the connection url
     * @return 
     */
    public String getUrl() {
    	return this.url;
    }
    /**
     * This method sets the database username.
     * @param user The username to set.
     */
    public void setUser(String user) {
    	this.user = user;
    }
    
    /**
     * This method returns the database user.
     * @return. Returns database user.
     */
    public String getUser() {
    	return this.user;
    }
        
    /**
     * This method sets the database password.
     * @param password The password to set.
     */
    public void setPassword(String password) {
    	this.passwd = password;
    }
 
    /**
     * This method returns the database password. 
     * @return. Returns the database password.
     */
    public String getPassword() {
    	return this.passwd;
    }
            
    /**
     * Return connection
     * @return
     */
    public Connection getConnection() {
    	try {
    		if (this.connection == null || this.connection.isClosed()) {
    			this.connection = connectDB();
    		}
    	}
    	catch (SQLException sqle) {
    		log.error("SQLException in getConnection: Message: [{}] Errorcode:[{}] SQLState:[{}] ",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
    		this.connection = null;
    	}
    	return this.connection;
    }
        
    /**
     * This method dumps JdbcInfo settings. 
     *
     */
    public void dumpJdbcInfo() {
        String conn = "";
        if (connection == null) {conn = "No connection";} else {conn = connection.toString();}
        log.debug("Driver:[{}] Url:[{}] User:[{}] Connection:[{}]",getDriver(),getUrl(),getUser(),conn);
    }

    /**
     * Open connection with the database.
     * @return jdbc connection
     */
    public Connection connectDB() {

        driver = getDriver();
        url = getUrl();

        // set host and driver params
        if (log.isDebugEnabled()) {
        	log.debug("Jdbc driver:[{}]",driver);
           	log.debug("Jdbc connection url:[{}]",url);
        }            
                
        try {
            // Load database driver if not already loaded
            Class.forName(driver);
            // Establish network connection to database.
            
            this.connection = null;
            Properties props = new Properties();
            if (getUser() != null) {
            	props.setProperty("user", getUser());
            	props.setProperty("password", getPassword());
                this.connection = DriverManager.getConnection(url, props);
            }
            else {
                this.connection = DriverManager.getConnection(url);
            }
            this.connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            this.connection.setAutoCommit(false);

            if (log.isDebugEnabled()) {
            	DatabaseMetaData dbMetaData = this.connection.getMetaData();
            	log.debug("Connection to database succesfull established to database name:[{}] version:[{}]",dbMetaData.getDatabaseProductName(),dbMetaData.getDatabaseProductVersion());
            }
            return this.connection;       
        }
        catch(ClassNotFoundException cnfe) {
            log.error("Error loading driver:[{}] ",cnfe);
            return null;
        }
        catch(SQLException sqle) {
        	log.error("SQLException in doDbConnect: Message:[{}] Errorcode:[{}] SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        	dumpJdbcInfo();
            return null;
        }
    }
    
    /**
     * Disconnection from the database
     */
    public void disconnectDB() {
    	   
    	try {
    		if (this.connection != null && this.connection.isClosed() == false) {
    			this.connection.close();
    			if (log.isDebugEnabled()) {log.debug("Connection succesfull closed");}
    		}
    	}
    	catch (SQLException sqle) {
        	log.error("SQLException in disconnectDB: Message:[{}] Errorcode:[{}]  SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
        	this.connection = null;
    	}
    }
    
    
    /**
     * Return ResultSet for given selectquery
     * @param query The select query.
     * @return ResultSet. In case of no connection null returned
     */
    public ResultSet doSelectQuery(String query) throws SQLException {
        
    	try {
            // Check for Db connection
    		if (getConnection() == null) {
    			log.debug("Unable to setup connection");
    			return null;
            }
                        
            // create statement
            Statement statement = this.connection.createStatement();
            // Send query to database and store results
            ResultSet rs = statement.executeQuery(query);

            if (log.isDebugEnabled()) {
                // Look up information about a particular table
                ResultSetMetaData rsMetaData = rs.getMetaData();
                log.debug("SQLQuery executed:[{}] Column count:[{}] Auto commit mode:[{}] Transaction isolation:[{}]"
                        ,query,rsMetaData.getColumnCount(),this.connection.getAutoCommit(),this.connection.getTransactionIsolation());
            }
            
            return rs;
        }
        catch(SQLException sqle) {
    		log.error("SQLException in doDBQuery: Message:[{}] Errorcode:[{}]  SQLState:[{}]",sqle.getMessage(),sqle.getErrorCode(),sqle.getSQLState());
            log.error("SQLQuery:[{}]",query);
        	dumpJdbcInfo();
            return null;
        }
    }
}
