package org.opencb.bionetdb.app.cli.admin.executors;

import org.opencb.bionetdb.app.cli.CommandExecutor;
import org.opencb.bionetdb.app.cli.admin.AdminCliOptionsParser;
import org.opencb.bionetdb.core.config.DownloadProperties;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.commons.utils.URLUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by jtarraga on 19/10/20.
 */
public class DownloadCommandExecutor extends CommandExecutor {

    private AdminCliOptionsParser.DownloadCommandOptions downloadCommandOptions;

    public DownloadCommandExecutor(AdminCliOptionsParser.DownloadCommandOptions downloadCommandOptions) {
        super(downloadCommandOptions.commonOptions.logLevel, downloadCommandOptions.commonOptions.conf);

        this.downloadCommandOptions = downloadCommandOptions;
    }

    @Override
    public void execute() throws BioNetDBException {
        if (downloadCommandOptions.outDir == null || downloadCommandOptions.outDir.isEmpty()) {
            throw new BioNetDBException("Missing output directory when executing the download command");
        }

        if (configuration.getDownload() == null) {
            throw new BioNetDBException("Missing the download section in BioNetDB configuration file");
        }

        Path outDir = Paths.get(downloadCommandOptions.outDir);
        DownloadProperties download = configuration.getDownload();

        // Gene
        if (download.getGene() != null) {
            try {
                URLUtils.download(new URL(download.getGene().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Protein
        if (download.getProtein() != null) {
            try {
                URLUtils.download(new URL(download.getProtein().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Panel
        if (download.getPanel() != null) {
            try {
                URLUtils.download(new URL(download.getPanel().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Clinvar
        if (download.getClinvar() != null) {
            try {
                URLUtils.download(new URL(download.getClinvar().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

        // Reactome
        if (download.getReactome() != null) {
            try {
                URLUtils.download(new URL(download.getReactome().getHost()), outDir);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
