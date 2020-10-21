/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.bionetdb.client.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by imedina on 12/05/16.
 */
public class ClientConfiguration {

    private String version;
    private String logLevel;
    private RestConfig rest;

    public ClientConfiguration() {
    }

    public static ClientConfiguration load(InputStream configurationInputStream) throws IOException {
        return load(configurationInputStream, "YAML");
    }

    public static ClientConfiguration load(InputStream configurationInputStream, String format)
        throws IOException {
        ClientConfiguration clientConfiguration;
        ObjectMapper objectMapper;
        switch (format.toUpperCase()) {
            case "JSON":
                objectMapper = new ObjectMapper();
                clientConfiguration = objectMapper.readValue(configurationInputStream, ClientConfiguration.class);
                break;
            case "YML":
            case "YAML":
            default:
                objectMapper = new ObjectMapper(new YAMLFactory());
                clientConfiguration = objectMapper.readValue(configurationInputStream, ClientConfiguration.class);
                break;
        }

        return clientConfiguration;
    }

    public void serialize(OutputStream configurationOutputStream) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper(new YAMLFactory());
        jsonMapper.writerWithDefaultPrettyPrinter().writeValue(configurationOutputStream, this);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClientConfiguration{");
        sb.append("version='").append(version).append('\'');
        sb.append(", logLevel='").append(logLevel).append('\'');
        sb.append(", rest=").append(rest);
        sb.append('}');
        return sb.toString();
    }

    public String getVersion() {
        return version;
    }

    public ClientConfiguration setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public ClientConfiguration setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }

    public RestConfig getRest() {
        return rest;
    }

    public ClientConfiguration setRest(RestConfig rest) {
        this.rest = rest;
        return this;
    }
}
