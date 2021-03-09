package org.opencb.bionetdb.lib;

import htsjdk.variant.variantcontext.VariantContext;
import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.models.network.NetworkPath;
import org.opencb.bionetdb.lib.analysis.InterpretationAnalysis;
import org.opencb.bionetdb.lib.analysis.NetworkAnalysis;
import org.opencb.bionetdb.lib.analysis.VariantAnalysis;
import org.opencb.bionetdb.lib.analysis.interpretation.TieringInterpretationAnalysis;
import org.opencb.bionetdb.lib.api.NetworkDBAdaptor;
import org.opencb.bionetdb.lib.api.iterators.NetworkPathIterator;
import org.opencb.bionetdb.lib.api.iterators.RowIterator;
import org.opencb.bionetdb.lib.db.Neo4JNetworkDBAdaptor;
import org.opencb.bionetdb.lib.executors.NetworkQueryExecutor;
import org.opencb.bionetdb.lib.executors.NodeQueryExecutor;
import org.opencb.bionetdb.lib.executors.PathQueryExecutor;
import org.opencb.bionetdb.lib.utils.Builder;
import org.opencb.bionetdb.lib.utils.Downloader;
import org.opencb.bionetdb.lib.utils.Importer;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by joaquin on 1/29/18.
 */
public class BioNetDbManager {

    private BioNetDBConfiguration configuration;

    private NetworkDBAdaptor networkDBAdaptor;

    private Logger logger;

    private static final int QUERY_MAX_RESULTS = 50000;
    private TieringInterpretationAnalysis tieringInterpretationAnalysis;

    public BioNetDbManager(BioNetDBConfiguration configuration) throws BioNetDBException {
        // We first create te logger to debug next actions
        logger = LoggerFactory.getLogger(BioNetDbManager.class);

        // We check that the configuration exists and the databases are not empty
        if (configuration == null || configuration.getDatabase() == null) {
            logger.error("BioNetDB configuration is null or database is empty");
            throw new BioNetDBException("BioNetDBConfiguration is null or database is empty");
        }
        this.configuration = configuration;

        // We can now create the default NetworkDBAdaptor
//        boolean createIndex = false; // true
        networkDBAdaptor = new Neo4JNetworkDBAdaptor(this.configuration);
//        tieringInterpretationAnalysis = new TieringInterpretationAnalysis(((Neo4JNetworkDBAdaptor) this.networkDBAdaptor).getDriver());
    }

    //-------------------------------------------------------------------------

    public void download(Path outDir) throws IOException {
        FileUtils.checkDirectory(outDir);

        Downloader downloader = new Downloader(configuration.getDownload(), outDir);
        downloader.download();
    }

    //-------------------------------------------------------------------------

    public void build(Path inputPath, Path outputPath, List<String> exclude) throws IOException, NoSuchAlgorithmException {
        build(inputPath, outputPath, null, null, exclude);
    }

    public void build(Path inputPath, Path outputPath, List<String> variantFiles, List<String> networkFiles, List<String> exclude)
            throws IOException, NoSuchAlgorithmException {
        Builder builder = new Builder(inputPath, outputPath, parseFilters(exclude));
        if (CollectionUtils.isNotEmpty(variantFiles)) {
            builder.setAdditionalVariantFiles(variantFiles);
        }
        if (CollectionUtils.isNotEmpty(networkFiles)) {
            builder.setAdditionalNeworkFiles(networkFiles);
        }
        builder.build();
    }

    //-------------------------------------------------------------------------

    public void load(String database, Path inputPath) {
        System.out.println("Importing data into BioNetDB database...");
        Importer importer = new Importer(database, inputPath);
        importer.run();
        System.out.println("Importing data into BioNetDB database done!!");


        while (!importer.isRunning()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
        }

        networkDBAdaptor = new Neo4JNetworkDBAdaptor(this.configuration);
        System.out.println("Checking if database is ready.");
        while (!((Neo4JNetworkDBAdaptor) networkDBAdaptor).isReady()) {
            System.out.println("Not yet!");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
        }
        System.out.println("Now BioNetDB database is ready to be indexed.");

        System.out.println("Indexing BioNetDB database...");
        networkDBAdaptor.index();
        networkDBAdaptor.close();
        System.out.println("Indexing BioNetDB database done!!");
    }


    //---------------------------------------------
    // E X E C U T O R S
    //---------------------------------------------

    public NodeQueryExecutor getNodeQueryExecutor() {
        return new NodeQueryExecutor(networkDBAdaptor);
    }

    public PathQueryExecutor getPathQueryExecutor() {
        return new PathQueryExecutor(networkDBAdaptor);
    }

    public NetworkQueryExecutor getNetworkQueryExecutor() {
        return new NetworkQueryExecutor(networkDBAdaptor);
    }

    //---------------------------------------------
    // A N A L Y S I S
    //---------------------------------------------

    public NetworkAnalysis getNetworkAnalysis() {
        return new NetworkAnalysis(networkDBAdaptor);
    }

    public VariantAnalysis getVariantAnalysis() {
        return new VariantAnalysis(networkDBAdaptor);
    }

    public InterpretationAnalysis getInterpretationAnalysis() {
        return new InterpretationAnalysis(networkDBAdaptor);
    }

    //---------------------------------------------

    public void close() throws Exception {
        networkDBAdaptor.close();
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private List<Variant> convert(List<VariantContext> variantContexts, VariantContextToVariantConverter converter) {
        // Iterate over variant context and convert to variant
        List<Variant> variants = new ArrayList<>(variantContexts.size());
        for (VariantContext variantContext : variantContexts) {
            Variant variant = converter.convert(variantContext);
            variants.add(variant);
        }
        return variants;
    }


    private DataResult<List<Object>> getQueryResult(RowIterator rowIterator) {
        List<List<Object>> rows = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        while (rowIterator.hasNext()) {
            if (rows.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            rows.add(rowIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new DataResult(time, Collections.emptyList(), rows.size(), rows, rows.size());
    }

    public QueryResult<NetworkPath> getQueryResult(NetworkPathIterator networkPathIterator)
            throws BioNetDBException {
        List<NetworkPath> networkPaths = new ArrayList<>();

        long startTime = System.currentTimeMillis();
        while (networkPathIterator.hasNext()) {
            if (networkPaths.size() >= this.QUERY_MAX_RESULTS) {
                break;
            }
            networkPaths.add(networkPathIterator.next());
        }
        long stopTime = System.currentTimeMillis();

        int time = (int) (stopTime - startTime) / 1000;
        return new QueryResult("get", time, networkPaths.size(), networkPaths.size(), null, null, networkPaths);
    }

    public Map<String, Set<String>> parseFilters(List<String> excludeList) {
        Map<String, Set<String>> filters = null;
        if (CollectionUtils.isNotEmpty(excludeList)) {
            filters = new HashMap<>();
            for (String exclude: excludeList) {
                String[] split = exclude.split(":");
                if (split.length == 2) {
                    if (!filters.containsKey(split[0])) {
                        filters.put(split[0], new HashSet<>());
                    }
                    filters.get(split[0]).add(split[1]);
                }
            }
        }
        return filters;
    }
}
