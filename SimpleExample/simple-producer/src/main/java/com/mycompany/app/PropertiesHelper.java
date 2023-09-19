package com.mycompany.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesHelper {
    /**
     * Gets a Properties object that contains the keys and values defined
     * in the file src/main/resources/config.properties
     *
     * @return a {@link java.util.Properties} object
     * @throws Exception Thrown if the file config.properties is not available
     *                   in the directory src/main/resources
     */
    public static Properties getProperties() throws IOException {

        Properties props = null;
        // try to load the file config.properties
        try (InputStream input = KafkaRelayer.class.getClassLoader().getResourceAsStream("config.properties")) {

            props = new Properties();

            if (input == null) {
                throw new IOException("Sorry, unable to find config.properties");
            }

            // load a properties file from class path, inside static method
            props.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        return props;
    }

}
