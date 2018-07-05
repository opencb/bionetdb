package org.opencb.bionetdb.core.utils.cache;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.core.Xref;
import org.opencb.commons.utils.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GeneCache extends Cache<Gene> {

    private static Logger logger;

    public GeneCache() {
        super();

        objReader = objMapper.reader(Gene.class);
        logger = LoggerFactory.getLogger(this.getClass());
    }

    @Override
    public void index(Path input, Path output) throws IOException {
        String objFilename = output.toString() + "/genes.rocksdb";
        String xrefObjFilename = output.toString() + "/xref.genes.rocksdb";

//        if (Paths.get(objFilename).toFile().exists()
//                && Paths.get(xrefObjFilename).toFile().exists()) {
//            objRocksDb = rocksDbManager.getDBConnection(objFilename, true);
//            xrefObjRocksDb = rocksDbManager.getDBConnection(xrefObjFilename, true);
//            logger.info("\tGene index already created!");
//            return;
//        }

        // Delete protein RocksDB files
        Paths.get(objFilename).toFile().delete();
        Paths.get(xrefObjFilename).toFile().delete();

        // Create gene RocksDB files (protein and xrefs)
        objRocksDb = rocksDbManager.getDBConnection(objFilename, true);
        xrefObjRocksDb = rocksDbManager.getDBConnection(xrefObjFilename, true);

        BufferedReader reader = org.opencb.commons.utils.FileUtils.newBufferedReader(input);
        String jsonGene = reader.readLine();
        long geneCounter = 0;
        while (jsonGene != null) {
            Gene gene = objReader.readValue(jsonGene);
            String geneId = gene.getId();
            if (StringUtils.isNotEmpty(geneId)) {
                geneCounter++;
                if (geneCounter % 5000 == 0) {
                    logger.info("Indexing {} genes...", geneCounter);
                }
                // Save gene
                rocksDbManager.putString(geneId, jsonGene, objRocksDb);

                // Save xrefs for that gene
                rocksDbManager.putString(geneId, geneId, xrefObjRocksDb);
                if (StringUtils.isNotEmpty(gene.getName())) {
                    rocksDbManager.putString(gene.getName(), geneId, xrefObjRocksDb);
                }

                if (ListUtils.isNotEmpty(gene.getTranscripts())) {
                    for (Transcript transcr : gene.getTranscripts()) {
                        if (StringUtils.isNotEmpty(transcr.getId()) || StringUtils.isNotEmpty(transcr.getName())) {
                            if (ListUtils.isNotEmpty(transcr.getXrefs())) {
                                for (Xref xref: transcr.getXrefs()) {
                                    if (org.apache.commons.lang.StringUtils.isNotEmpty(xref.getId())) {
                                        rocksDbManager.putString(xref.getId(), geneId, xrefObjRocksDb);
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                logger.info("Skipping indexing gene: missing gene ID from JSON file");
            }

            // Next line
            jsonGene = reader.readLine();
        }
        logger.info("Indexing {} genes. Done.", geneCounter);

        reader.close();
    }
}
