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

package org.opencb.bionetdb.core.config;

import java.util.List;

/**
 * Created by jtarraga on 20/10/20.
 */
public class DownloadProperties {

    private URLProperties network;
    private URLProperties ensemblGene;
    private URLProperties refSeqGene;
    private URLProperties protein;
    private URLProperties panel;
    private URLProperties clinicalVariant;

    public URLProperties getEnsemblGene() {
        return ensemblGene;
    }

    public DownloadProperties setEnsemblGene(URLProperties ensemblGene) {
        this.ensemblGene = ensemblGene;
        return this;
    }

    public URLProperties getRefSeqGene() {
        return refSeqGene;
    }

    public DownloadProperties setRefSeqGene(URLProperties refSeqGene) {
        this.refSeqGene = refSeqGene;
        return this;
    }

    public URLProperties getProtein() {
        return protein;
    }

    public DownloadProperties setProtein(URLProperties protein) {
        this.protein = protein;
        return this;
    }

    public URLProperties getPanel() {
        return panel;
    }

    public DownloadProperties setPanel(URLProperties panel) {
        this.panel = panel;
        return this;
    }

    public URLProperties getClinicalVariant() {
        return clinicalVariant;
    }

    public DownloadProperties setClinicalVariant(URLProperties clinicalVariant) {
        this.clinicalVariant = clinicalVariant;
        return this;
    }

    public URLProperties getNetwork() {
        return network;
    }

    public DownloadProperties setNetwork(URLProperties network) {
        this.network = network;
        return this;
    }

    public static class URLProperties {

        private String host;
        private String version;
        private List<String> files;

        public String getHost() {
            return host;
        }

        public void setHostclinvar(String host) {
            this.host = host;
        }

        public String getVersion() {
            return version;
        }

        public URLProperties setVersion(String version) {
            this.version = version;
            return this;
        }

        public List<String> getFiles() {
            return files;
        }

        public URLProperties setFiles(List<String> files) {
            this.files = files;
            return this;
        }
    }
}
