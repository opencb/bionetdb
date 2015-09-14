package org.opencb.bionetdb.core.io;

import org.opencb.bionetdb.core.models.Xref;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by dapregi on 24/08/15.
 */
public class XrefAnnotationParser {

    /**
     * This method parses annotation files in tabular format with commented header:
     *
     *     e.g.:
     *         #name    source  id   sourceVersion  idVersion
     *         Entity1  DB1     ID1  db1            id1
     *         Entity2  DB2     ID2
     *         Entity3  DB3     ID3  db3
     *
     * - Field 1: entity reference id for which the new info will be added.
     * - Field 2: database name
     * - Field 3: entity id
     * - Field 4: database version
     * - Field 5: id version
     *
     * First three fields are mandatory: "name", "database" and "id".
     * Unknown info is represented as an empty string.
     */
    public Map<String, Xref> parseXrefAnnotationFile(Path path) throws IOException {
        // Reading GZip input stream
        InputStream inputStream;
        if (path.toFile().getName().endsWith(".gz")) {
            inputStream = new GZIPInputStream(new FileInputStream(path.toFile()));
        } else {
            inputStream = Files.newInputStream(path);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        Map<String, Xref> xrefAnnotation = new HashMap<>();
        String line;
        while ((line = reader.readLine()) != null) {
            Xref xref = new Xref();
            List<String> items = Arrays.asList(line.split("\t"));
            if (!line.startsWith("#")) {
                xref.setSource(items.get(1));
                xref.setId(items.get(2));
                if (items.size() > 3) {
                    xref.setSourceVersion(items.get(3));
                    xref.setIdVersion(items.get(4));
                }
                xrefAnnotation.put(items.get(0), xref);
            }
        }

        reader.close();
        inputStream.close();
        return xrefAnnotation;
    }
}
