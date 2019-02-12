package nl.ease2pay.postalcode.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Datasource {
	private String dsn;
	private String driver;
	private String url;
	private String user;
	private String password;
	private String database;
	
	/** Logger */
	private static Logger log = LoggerFactory.getLogger(Datasource.class);
	
	public Datasource() {}
	
	public Datasource(Properties props, String dsn) {
		setDsn(dsn);
		setDSNFromProperties(props, dsn);
	}
	
	public Datasource(String dsn) {
		setDsn(dsn);
	}

	public String getDsn() {
		return dsn;
	}

	public void setDsn(String dsn) {
		this.dsn = dsn;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}
	
	private void setDSNFromProperties(Properties properties, String dsn) {
		if (properties != null && dsn !=  null) {
			String key = "db"+"."+dsn+"."+"driver";
			setDriver(properties.getProperty(key,""));
			key = "db"+"."+dsn+"."+"url";
			setUrl(properties.getProperty(key,""));
			key = "db"+"."+dsn+"."+"user";
			setUser(properties.getProperty(key,""));
			key = "db"+"."+dsn+"."+"password";
			setPassword(properties.getProperty(key,""));
			key = "db"+"."+dsn+"."+"database";
			setDatabase(properties.getProperty(key,""));
		}
	}
	
	public void logDSN() {
		log.debug("Dsn name:[{}] ",getDsn());
		log.debug("[{}] database driver [{}]:",getDsn(),getDriver());
		log.debug("[{}] database url [{}]:",getDsn(),getUrl());
		log.debug("[{}] database user [{}]:",getDsn(),getUser());
		log.debug("[{}] database password: [{}]",getDsn(),getPassword());
		log.debug("[{}] database name: [{}]",getDsn(),getDatabase());
	}
 	
}
