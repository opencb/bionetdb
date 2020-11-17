package org.opencb.bionetdb.core.config;

/**
 * Created by imedina on 05/10/15.
 */
public class DatabaseConfiguration {

//    private String id;
//    private String species;

    private String host;
    private int port;
    private String user;
    private String password;

    /**
     * options parameter defines database-specific parameters.
     */
//    private ObjectMap options;


    public DatabaseConfiguration() {
    }

//    @Deprecated
//    public DatabaseConfiguration(String id, ObjectMap options) {
//        this.id = id;
//        this.options = options;
//    }


    public DatabaseConfiguration(String host, int port, String user, String password) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DatabaseConfiguration{");
        sb.append("host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getHost() {
        return host;
    }

    public DatabaseConfiguration setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public DatabaseConfiguration setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUser() {
        return user;
    }

    public DatabaseConfiguration setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public DatabaseConfiguration setPassword(String password) {
        this.password = password;
        return this;
    }
}
