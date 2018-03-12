package org.opencb.bionetdb.core.config;

import org.opencb.commons.datastore.core.ObjectMap;

/**
 * Created by imedina on 05/10/15.
 */
public class DatabaseConfiguration {

    private String id;
    private String species;

    private String host;
    private int port;
    private String user;
    private String password;

    /**
     * options parameter defines database-specific parameters.
     */
    private ObjectMap options;


    public DatabaseConfiguration() {
    }

    @Deprecated
    public DatabaseConfiguration(String id, ObjectMap options) {
        this.id = id;
        this.options = options;
    }

    public DatabaseConfiguration(String id, String species, String host, int port, String user, String password, ObjectMap options) {
        this.id = id;
        this.species = species;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DatabaseConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", species='").append(species).append('\'');
        sb.append(", host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public DatabaseConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public String getSpecies() {
        return species;
    }

    public DatabaseConfiguration setSpecies(String species) {
        this.species = species;
        return this;
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

    public ObjectMap getOptions() {
        return options;
    }

    public DatabaseConfiguration setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
