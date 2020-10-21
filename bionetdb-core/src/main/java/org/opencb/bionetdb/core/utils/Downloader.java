package org.opencb.bionetdb.core.utils;

import org.opencb.bionetdb.core.config.DownloadProperties;
import org.opencb.commons.utils.URLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

public class Downloader {

    private DownloadProperties downloadProperties;
    private Path outDir;

    private Logger logger;

    public Downloader() {
        this.logger = LoggerFactory.getLogger(this.getClass().toString());
    }

    public Downloader(DownloadProperties downloadProperties, Path outDir) {
        this.downloadProperties = downloadProperties;
        this.outDir = outDir;

        this.logger = LoggerFactory.getLogger(this.getClass().toString());
    }

    public void download() {
        // Gene
        if (downloadProperties.getGene() != null) {
            try {
                System.out.println("Downloading " + downloadProperties.getGene().getHost() + "...");
                URLUtils.download(new URL(downloadProperties.getGene().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Protein
        if (downloadProperties.getProtein() != null) {
            try {
                System.out.println("Downloading " + downloadProperties.getProtein().getHost() + "...");
                URLUtils.download(new URL(downloadProperties.getProtein().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Panel
        if (downloadProperties.getPanel() != null) {
            try {
                System.out.println("Downloading " + downloadProperties.getPanel().getHost() + "...");
                URLUtils.download(new URL(downloadProperties.getPanel().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Clinvar
        if (downloadProperties.getClinvar() != null) {
            try {
                System.out.println("Downloading " + downloadProperties.getClinvar().getHost() + "...");
                URLUtils.download(new URL(downloadProperties.getClinvar().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Reactome
        if (downloadProperties.getReactome() != null) {
            try {
                System.out.println("Downloading " + downloadProperties.getReactome().getHost() + "...");
                URLUtils.download(new URL(downloadProperties.getReactome().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Downloader{");
        sb.append("downloadProperties=").append(downloadProperties);
        sb.append(", outDir=").append(outDir);
        sb.append('}');
        return sb.toString();
    }

    public DownloadProperties getDownloadProperties() {
        return downloadProperties;
    }

    public Downloader setDownloadProperties(DownloadProperties downloadProperties) {
        this.downloadProperties = downloadProperties;
        return this;
    }

    public Path getOutDir() {
        return outDir;
    }

    public Downloader setOutDir(Path outDir) {
        this.outDir = outDir;
        return this;
    }
}
