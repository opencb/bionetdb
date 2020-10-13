package org.opencb.bionetdb.core.neo4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;

import java.io.IOException;

import static org.neo4j.procedure.Mode.SCHEMA;

public class UserDefinedProcedure {

    // Procedure classes. This static field is the configuration we use
    // to create full-text indexes.

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, `neo4j.log`
    @Context
    public Log log;

    /**
     * This is the second procedure defined in this class, it is used to update the
     * index with nodes that should be queryable. You can send the same node multiple
     * times, if it already exists in the index the index will be updated to match
     * the current state of the node.
     *
     *
     * Two, it returns {@code void} rather than a stream. This is a short-hand
     * for saying our procedure always returns an empty stream of empty records.
     *
     * Three, it uses a default value for the property list, in this way you can call
     * the procedure by invoking {@code CALL index(nodeId)}. Default values are
     * are provided as the Cypher string representation of the given type, e.g.
     * {@code {default: true}}, {@code null}, or {@code -1}.
     *
     * @param caJson JSON string for the clinical analysis to load
     */
    @Procedure(name = "org.opencb.bionetdb.core.neo4j.loadClinicalAnalysis", mode = SCHEMA)
    public void loadClinicalAnalysis(@Name("caJson") String caJson) {
        Neo4JLoader neo4JLoader = new Neo4JLoader(db, log);

        try (Transaction tx = db.beginTx()) {
            ObjectMapper defaultObjectMapper = JacksonUtils.getDefaultObjectMapper();
            ClinicalAnalysis ca = defaultObjectMapper.readValue(caJson, ClinicalAnalysis.class);
            neo4JLoader.loadClinicalAnalysis(ca);

            tx.success();
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("Done!");
    }
}
