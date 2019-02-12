package nl.ease2pay.postalcode.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;

public class Datasources {
	
	/** Logger */
	private static Logger log = LoggerFactory.getLogger(Datasources.class);
	
	private ArrayList<Datasource>dsns = new ArrayList<Datasource>();
	
	/**
	 * Constructor Datasources
	 * @param props The Datasource properties from the properties file.
	 */
	public Datasources(Properties props) {
		if (props != null) {
			ArrayList<String> names = findDatasourcesNames(props);
			for(String name:names) {
				Datasource dsn = new Datasource(props, name);
				if (dsn != null) {
					dsns.add(dsn);
					if (log.isDebugEnabled()) {
						log.debug("Datasource added to list:[{}]",dsn.getDsn());
					}
				}
			}
		}
	}
		
	/**
	 * This function returns a Datasource by datasource name
	 * @param dsnName The datasource name to search for.
	 * @return Datasource. in case not found null will be returned.
	 */
	public Datasource getDatasourceByName(String dsnName) {
		Iterator<Datasource> itr = dsns.iterator();
		while (itr.hasNext()) {
			Datasource dsn = itr.next(); 
			if (dsn != null && dsn.getDsn().startsWith(dsnName)) {
				return dsn;
			}
		}
		return null;
	}


	/**
	 * Find datasource names from the properties file.
	 * @param props The properties to be search for in the properties
	 * @return
	 */
	protected ArrayList<String> findDatasourcesNames(Properties props) {
		//syntax: db.mongodb.carlink.url
		//        db.carlink.url

		ArrayList<String> names = new ArrayList<>();
		Enumeration<Object> keys = props.keys();
		while (keys.hasMoreElements()) {
			String key = (String)keys.nextElement();
			if (key.endsWith(".url")) {
				//decode key.
				names.add(key.substring(3, key.length()-4));
				if (log.isDebugEnabled())
					log.debug("Datasource name found:[{}]",key.substring(3, key.length()-4));
			}
		}

		return names;
	}

}
