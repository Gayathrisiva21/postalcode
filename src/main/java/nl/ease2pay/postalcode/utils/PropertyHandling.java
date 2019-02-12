package nl.ease2pay.postalcode.utils;

import nl.ease2pay.postalcode.ProcessUpdateMO;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyHandling {

    private static Logger LOGGER = Logger.getLogger(ProcessUpdateMO.class);

    public static Properties readProperties(String filename) {
        Properties props = new Properties();
        try {
            //load a properties file from class path, inside static method
            InputStream in = new FileInputStream(filename);
            props.load(in);
        } catch (IOException ioe) {
            LOGGER.error("IOException " + ioe.getMessage());
            props = null;
        }
        return props;
    }

}
