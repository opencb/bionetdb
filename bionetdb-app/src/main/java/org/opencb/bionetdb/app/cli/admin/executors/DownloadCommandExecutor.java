package org.opencb.bionetdb.app.cli.admin.executors;

import org.opencb.bionetdb.app.cli.CommandExecutor;
import org.opencb.bionetdb.app.cli.admin.AdminCliOptionsParser;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.lib.BioNetDbManager;
import org.opencb.bionetdb.lib.utils.Downloader;
import org.opencb.commons.utils.FileUtils;

import java.io.IOException;
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
    public void execute() throws BioNetDBException, IOException {
        if (downloadCommandOptions.outDir == null || downloadCommandOptions.outDir.isEmpty()) {
            throw new BioNetDBException("Missing output directory when executing the download command");
        }
        FileUtils.checkDirectory(Paths.get(downloadCommandOptions.outDir));

        if (configuration.getDownload() == null) {
            throw new BioNetDBException("Missing the download section in BioNetDB configuration file");
        }

        BioNetDbManager manager = new BioNetDbManager(configuration);
        manager.download(Paths.get(downloadCommandOptions.outDir));
    }
}
