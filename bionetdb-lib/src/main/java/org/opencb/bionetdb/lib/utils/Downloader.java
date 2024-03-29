package org.opencb.bionetdb.lib.utils;

import org.opencb.bionetdb.core.config.DownloadProperties;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.URLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

    public void download() throws IOException {
        FileUtils.checkDirectory(outDir);

        // Ensembl gene
        if (downloadProperties.getEnsemblGene() != null) {
            try {
                downloadURL(new URL(downloadProperties.getEnsemblGene().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // RefSeq gene
        if (downloadProperties.getRefSeqGene() != null) {
            try {
                downloadURL(new URL(downloadProperties.getRefSeqGene().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Protein
        if (downloadProperties.getProtein() != null) {
            try {
                downloadURL(new URL(downloadProperties.getProtein().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Panel
        if (downloadProperties.getPanel() != null) {
            try {
                downloadURL(new URL(downloadProperties.getPanel().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Clinical variant
        if (downloadProperties.getClinicalVariant() != null) {
            try {
                downloadURL(new URL(downloadProperties.getClinicalVariant().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Reactome
        if (downloadProperties.getNetwork() != null) {
            try {
                downloadURL(new URL(downloadProperties.getNetwork().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

    }

    private void downloadURL(URL url, Path outDir) throws IOException {
        String filename = new File(url.getFile()).getName();
        if (outDir.resolve(filename).toFile().exists()) {
            logger.info("Skipping to downloadd " + filename + ". It already exists in " + outDir);
        } else {
            logger.info("Downloading " + url + "...");
            URLUtils.download(url, outDir);
            logger.info(url + " downloaded!");
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
