package org.opencb.bionetdb.app.cli;

/**
 * Created by pfurio on 24/08/15.
 */
public class AnnotateCommandExecutor extends CommandExecutor {

    private CliOptionsParser.VariantAnnotationCommandOptions annotateCommandOptions;

    public AnnotateCommandExecutor(CliOptionsParser.VariantAnnotationCommandOptions annotateCommandOptions) {
        super(annotateCommandOptions.commonOptions.logLevel, annotateCommandOptions.commonOptions.conf);

        this.annotateCommandOptions = annotateCommandOptions;
    }

    @Override
    public void execute() {
/*
        try {
            NetworkDBAdaptor networkDBAdaptor = new Neo4JNetworkDBAdaptor(annotateCommandOptions.database);

            // Parse input file...

            // For each node, add Xref annotations...
            // networkDBAdaptor.addXrefs( n, xref...);

        } catch (IOException e) {
            e.printStackTrace();
        }
*/

    }

}
