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

/**
 * Created by imedina on 04/05/16.
 */
public class RestConfig {

    private String host;
    private int timeout;

    public RestConfig() {
    }

    public RestConfig(String host, int timeout) {
        this.host = host;
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RestConfig{");
        sb.append("host=").append(host);
        sb.append(", timeout=").append(timeout);
        sb.append('}');
        return sb.toString();
    }

    public String getHosts() {
        return host;
    }

    public RestConfig setHosts(String host) {
        this.host = host;
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public RestConfig setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }
}
