package org.jax.mvar.insert;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private String url;
    private String user;
    private String password;

    public Config() {
        try (InputStream input = getClass()
                .getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();
            // load a properties file
            prop.load(input);
            // get the property value
            this.url = prop.getProperty("url");
            this.user = prop.getProperty("user");
            this.password = prop.getProperty("password");

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

}