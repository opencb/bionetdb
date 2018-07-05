package org.opencb.bionetdb.core.utils.cache;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.DbReferenceType;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.Entry;
import org.opencb.biodata.formats.protein.uniprot.v201504jaxb.EvidencedStringType;
import org.opencb.commons.utils.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ProteinCache extends Cache<Entry> {

    protected static Logger logger;

    public ProteinCache() {
        super();

        objReader = objMapper.reader(Entry.class);
        logger = LoggerFactory.getLogger(this.getClass());
    }

    @Override
    public void index(Path input, Path output) throws IOException {
        String objFilename = output.toString() + "/proteins.rocksdb";
        String xrefObjFilename = output.toString() + "/xref.proteins.rocksdb";

//        if (Paths.get(objFilename).toFile().exists()
//                && Paths.get(xrefObjFilename).toFile().exists()) {
//            objRocksDb = rocksDbManager.getDBConnection(objFilename, true);
//            xrefObjRocksDb = rocksDbManager.getDBConnection(xrefObjFilename, true);
//            logger.info("\tProtein index already created!");
//            return;
//        }

        // Delete protein RocksDB files
        Paths.get(objFilename).toFile().delete();
        Paths.get(xrefObjFilename).toFile().delete();

        // Create gene RocksDB files (protein and xrefs)
        objRocksDb = rocksDbManager.getDBConnection(objFilename, true);
        xrefObjRocksDb = rocksDbManager.getDBConnection(xrefObjFilename, true);

        BufferedReader reader = org.opencb.commons.utils.FileUtils.newBufferedReader(input);
        String jsonProtein = reader.readLine();
        long proteinCounter = 0;
        while (jsonProtein != null) {
            Entry protein = objReader.readValue(jsonProtein);
            if (ListUtils.isNotEmpty(protein.getAccession())) {
                proteinCounter++;
                if (proteinCounter % 5000 == 0) {
                    logger.info("Indexing {} proteins...", proteinCounter);
                }

                // Save protein in RocksDB
                String proteinAcc = protein.getAccession().get(0);
                rocksDbManager.putString(proteinAcc, jsonProtein, objRocksDb);

                // Save protein xrefs
                if (ListUtils.isNotEmpty(protein.getAccession())) {
                    for (String acc: protein.getAccession()) {
                        rocksDbManager.putString(acc, proteinAcc, xrefObjRocksDb);
                    }
                }

                try {
                    for (EvidencedStringType shortName : protein.getProtein().getRecommendedName().getShortName()) {
                        if (StringUtils.isNotEmpty(shortName.getValue())) {
                            rocksDbManager.putString(shortName.getValue(), proteinAcc, xrefObjRocksDb);
                        }
                    }
                } catch (Exception e) {
                    logger.info(e.getLocalizedMessage());
                }

                if (ListUtils.isNotEmpty(protein.getName())) {
                    for (String name: protein.getName()) {
                        rocksDbManager.putString(name, proteinAcc, xrefObjRocksDb);
                    }
                }

                if (ListUtils.isNotEmpty(protein.getDbReference())) {
                    for (DbReferenceType dbRef: protein.getDbReference()) {
                        rocksDbManager.putString(dbRef.getId(), proteinAcc, xrefObjRocksDb);
                    }
                }
            } else {
                logger.info("Skipping indexing protein: missing protein accession from JSON file");
            }

            // Next line
            jsonProtein = reader.readLine();
        }
        logger.info("Indexing {} proteins. Done.", proteinCounter);

        reader.close();
    }
}
