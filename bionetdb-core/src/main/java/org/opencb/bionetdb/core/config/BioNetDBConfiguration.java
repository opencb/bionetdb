package org.opencb.bionetdb.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by imedina on 05/10/15.
 */
public class BioNetDBConfiguration {

    private String defaultDatabase;
    private String logLevel;
    private String logFile;

    private List<DatabaseConfiguration> databases;

    protected static Logger logger = LoggerFactory.getLogger(BioNetDBConfiguration.class);

    public BioNetDBConfiguration() {
        this("", new ArrayList<>());
    }

    public BioNetDBConfiguration(String defaultDatabase, List<DatabaseConfiguration> databaseConfigurations) {
        this.defaultDatabase = defaultDatabase;
        this.databases = databaseConfigurations;

    }

    /*
     * This method attempts to find and load the configuration from installation directory,
     * if not exists then loads JAR storage-configuration.yml.
     */
    @Deprecated
    public static BioNetDBConfiguration load() throws IOException {
        String appHome = System.getProperty("app.home", System.getenv("BIONETDB_HOME"));
        Path path = Paths.get(appHome + "/configuration.yml");
        if (appHome != null && Files.exists(path)) {
            logger.debug("Loading configuration from '{}'", appHome + "/configuration.yml");
            return BioNetDBConfiguration
                    .load(new FileInputStream(new File(appHome + "/configuration.yml")));
        } else {
            logger.debug("Loading configuration from '{}'",
                    BioNetDBConfiguration.class.getClassLoader()
                            .getResourceAsStream("configuration.yml")
                            .toString());
            return BioNetDBConfiguration
                    .load(BioNetDBConfiguration.class.getClassLoader().getResourceAsStream("configuration.yml"));
        }
    }

    public static BioNetDBConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "yaml");
    }

    public static BioNetDBConfiguration load(InputStream configurationInputStream, String format) throws IOException {
        BioNetDBConfiguration bioNetDBConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                bioNetDBConfiguration = objectMapper.readValue(configurationInputStream, BioNetDBConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                bioNetDBConfiguration = objectMapper.readValue(configurationInputStream, BioNetDBConfiguration.class);
                break;
        }

        return bioNetDBConfiguration;
    }

    public void serialize(OutputStream configurationOututStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOututStream, this);
    }

    public DatabaseConfiguration findDatabase() {
        if (defaultDatabase != null && !defaultDatabase.isEmpty()) {
            return findDatabase(defaultDatabase);
        } else {
            if (databases.size() > 0) {
                return databases.get(0);
            } else {
                return null;
            }
        }
    }

    public DatabaseConfiguration findDatabase(String database) {
        DatabaseConfiguration databaseConfiguration = null;
        for (DatabaseConfiguration databaseConf : databases) {
            if (databaseConf.getId().equals(database)) {
                databaseConfiguration = databaseConf;
                break;
            }
        }

        return databaseConfiguration;
    }

    public void addDatabase(InputStream configurationInputStream) throws IOException {
        addDatabase(configurationInputStream, "yaml");
    }

    public void addDatabase(InputStream configurationInputStream, String format) throws IOException {
        DatabaseConfiguration databaseConfiguration;
        ObjectMapper objectMapper;
        switch (format) {
            case "json":
                objectMapper = new ObjectMapper();
                databaseConfiguration = objectMapper.readValue(configurationInputStream, DatabaseConfiguration.class);
                break;
            case "yml":
            case "yaml":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                databaseConfiguration = objectMapper.readValue(configurationInputStream, DatabaseConfiguration.class);
                break;
        }

        if (databaseConfiguration != null) {
            this.databases.add(databaseConfiguration);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BioNetDBConfiguration{");
        sb.append("defaultDatabase='").append(defaultDatabase).append('\'');
        sb.append(", logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", databases=").append(databases);
        sb.append('}');
        return sb.toString();
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public void setDefaultDatabase(String defaultDatabase) {
        this.defaultDatabase = defaultDatabase;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public List<DatabaseConfiguration> getDatabases() {
        return databases;
    }

    public void setDatabases(List<DatabaseConfiguration> databases) {
        this.databases = databases;
    }
}
