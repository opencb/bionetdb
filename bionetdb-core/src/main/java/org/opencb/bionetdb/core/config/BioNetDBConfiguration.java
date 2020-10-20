package org.opencb.bionetdb.core.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.StringUtils;
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

    private String logLevel;
    private String logFile;

    private List<DatabaseConfiguration> databases;
    private DownloadProperties download;

    protected static Logger logger = LoggerFactory.getLogger(BioNetDBConfiguration.class);

    public BioNetDBConfiguration() {
        this(new ArrayList<>());
    }

    public BioNetDBConfiguration(List<DatabaseConfiguration> databaseConfigurations) {
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

    public DatabaseConfiguration findDatabase(String database) {
        DatabaseConfiguration databaseConfiguration = null;
        if (StringUtils.isNotEmpty(database)) {
            for (DatabaseConfiguration databaseConf : databases) {
                if (databaseConf.getId().equals(database)) {
                    databaseConfiguration = databaseConf;
                    break;
                }
            }
        } else {
            databaseConfiguration = this.databases.get(0);
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
        sb.append("logLevel='").append(logLevel).append('\'');
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append(", databases=").append(databases);
        sb.append(", download=").append(download);
        sb.append('}');
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public BioNetDBConfiguration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public String getLogFile() {
        return logFile;
    }

    public BioNetDBConfiguration setLogFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

    public List<DatabaseConfiguration> getDatabases() {
        return databases;
    }

    public BioNetDBConfiguration setDatabases(List<DatabaseConfiguration> databases) {
        this.databases = databases;
        return this;
    }

    public DownloadProperties getDownload() {
        return download;
    }

    public BioNetDBConfiguration setDownload(DownloadProperties download) {
        this.download = download;
        return this;
    }

    public static Logger getLogger() {
        return logger;
    }

    public static void setLogger(Logger logger) {
        BioNetDBConfiguration.logger = logger;
    }
}
