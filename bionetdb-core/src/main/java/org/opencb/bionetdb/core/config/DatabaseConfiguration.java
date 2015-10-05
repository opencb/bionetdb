package org.opencb.bionetdb.core.config;

import org.opencb.datastore.core.ObjectMap;

/**
 * Created by imedina on 05/10/15.
 */
public class DatabaseConfiguration {

    private String id;

    private String host;
    private int port;
    private String user;
    private String password;

    private String path;

    /**
     * options parameter defines database-specific parameters.
     */
    private ObjectMap options;


    public DatabaseConfiguration() {

    }

    public DatabaseConfiguration(String id, ObjectMap options) {
        this.id = id;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DatabaseConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
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

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public void setOptions(ObjectMap options) {
        this.options = options;
    }
}
