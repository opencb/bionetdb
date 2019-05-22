package org.opencb.bionetdb.core.neo4j;

import org.apache.commons.collections.MapUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.GenomicFeature;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.*;
import org.opencb.bionetdb.core.utils.NodeBuilder;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.Interpretation;
import org.parboiled.common.StringUtils;

import java.util.List;
import java.util.Map;

import static org.neo4j.graphdb.RelationshipType.withName;
import static org.opencb.bionetdb.core.models.network.Node.Type.*;
import static org.opencb.bionetdb.core.models.network.Node.Type.CONSERVATION;
import static org.opencb.bionetdb.core.models.network.Node.Type.FUNCTIONAL_SCORE;
import static org.opencb.bionetdb.core.models.network.Node.Type.GENE;
import static org.opencb.bionetdb.core.models.network.Node.Type.TRAIT_ASSOCIATION;
import static org.opencb.bionetdb.core.models.network.Relation.Type.*;
import static org.opencb.bionetdb.core.models.network.Relation.Type.PROTEIN;
import static org.opencb.bionetdb.core.models.network.Relation.Type.SO;
import static org.opencb.bionetdb.core.models.network.Relation.Type.TRANSCRIPT;

public class Neo4JLoader {

    private GraphDatabaseService graphDb;
    private Log log;

    public Neo4JLoader(GraphDatabaseService graphDb, Log log) {
        this.graphDb = graphDb;
        this.log = log;
    }

    public Node loadClinicalAnalysis(ClinicalAnalysis clinicalAnalysis) {
        Node caNode = graphDb.findNode(Label.label("CLINICAL_ANALYSIS"), "id", clinicalAnalysis.getId());
        if (caNode != null) {
            //log.info("Clinical analysis ID " + clinicalAnalysis.getId() + " already loaded. Skip.");
            return caNode;
        }

        caNode = createNeo4JNode(NodeBuilder.newNode(0, clinicalAnalysis));

        // Disorder
        if (clinicalAnalysis.getDisorder() != null) {
            // Disorder node and relation: clinical analysis - disorder
            Node disorderNode = loadDisorder(clinicalAnalysis.getDisorder());
            caNode.createRelationshipTo(disorderNode, withName(CLINICAL_ANALYSIS__DISORDER.toString()));
        }

        // Files
        if (MapUtils.isNotEmpty(clinicalAnalysis.getFiles())) {
            for (String key : clinicalAnalysis.getFiles().keySet()) {
                List<File> files = clinicalAnalysis.getFiles().get(key);
                if (CollectionUtils.isNotEmpty(files)) {
                    for (File file : files) {
                        Node fileNode = loadFile(file);
                        caNode.createRelationshipTo(fileNode, withName(CLINICAL_ANALYSIS__FILE.toString()));
                    }
                }
            }
        }

        // Family
        if (clinicalAnalysis.getFamily() != null) {
            // Family node and relation clinical analysis - family
            Node familyNode = loadFamily(clinicalAnalysis.getFamily());
            caNode.createRelationshipTo(familyNode, withName(CLINICAL_ANALYSIS__FAMILY.toString()));
        }

        // Proband
        if (clinicalAnalysis.getProband() != null) {
            // Proband node and relation clinical analysis - individual (proband)
            Node probandNode = loadIndividual(clinicalAnalysis.getProband());
            caNode.createRelationshipTo(probandNode, withName(PROBAND___CLINICAL_ANALYSIS___INDIVIDUAL.toString()));
        }


        // Clinical Analyst
        if (clinicalAnalysis.getAnalyst() != null) {
            // Analyst node and relation: clinical analysis - analyst
            Node analystNode = createNeo4JNode(NodeBuilder.newNode(0, clinicalAnalysis.getAnalyst()));
            caNode.createRelationshipTo(analystNode, withName(CLINICAL_ANALYSIS__CLINICAL_ANALYST.toString()));
        }

        // Comments
        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getComments())) {
            for (Comment comment : clinicalAnalysis.getComments()) {
                // Comment node and relation: clinical analysis - comment
                Node commentNode = createNeo4JNode(NodeBuilder.newNode(0, comment));
                caNode.createRelationshipTo(commentNode, withName(CLINICAL_ANALYSIS__COMMENT.toString()));
            }
        }

        // Alerts
        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getAlerts())) {
            for (Alert alert : clinicalAnalysis.getAlerts()) {
                // Alert node and relation: clinical analysis - alert
                Node alertNode = createNeo4JNode(NodeBuilder.newNode(0, alert));
                caNode.createRelationshipTo(alertNode, withName(CLINICAL_ANALYSIS__ALERT.toString()));
            }
        }

        // Interpretations
        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getInterpretations())) {
            for (Interpretation interpretation : clinicalAnalysis.getInterpretations()) {
                // Interpretation node and relation: clinical analysis - interpretation
                Node interpretationNode = loadInterpretation(interpretation);
                caNode.createRelationshipTo(interpretationNode, withName(CLINICAL_ANALYSIS__INTERPRETATION.toString()));
            }
        }
        return caNode;
    }

    public Node loadInterpretation(Interpretation interpretation) {
        Node interpretationNode = graphDb.findNode(Label.label(INTERPRETATION.name()), "id", interpretation.getId());
        if (interpretationNode != null) {
            //log.info("Interpretation ID " + interpretationNode.getId() + " already loaded. Skip.");
            return interpretationNode;
        }

        interpretationNode = createNeo4JNode(NodeBuilder.newNode(0, interpretation));

        // Software (dependencies)
        if (interpretation.getSoftware() != null) {
            // Software node and relation: interpretation - software
            Node softwareNode = loadSoftware(interpretation.getSoftware());
            interpretationNode.createRelationshipTo(softwareNode, withName(INTERPRETATION__SOFTWARE.toString()));
        }

        // Panels
        if (CollectionUtils.isNotEmpty(interpretation.getPanels())) {
            for (DiseasePanel panel : interpretation.getPanels()) {
                // Panel node and relation: interpretation - panel
                Node panelNode = loadPanel(panel);
                interpretationNode.createRelationshipTo(panelNode, withName(INTERPRETATION__PANEL.toString()));
            }
        }

        // Primary findings
        if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
            for (ReportedVariant primaryFinding : interpretation.getPrimaryFindings()) {
                // Primary finding node and relation: interpretation - primary finding
                Node findingNode = loadReportedVariant(primaryFinding);
                interpretationNode.createRelationshipTo(findingNode,
                        withName(PRIMARY_FINDING___INTERPRETATION___REPORTED_VARIANT.toString()));
            }
        }

        // Secondary findings
        if (CollectionUtils.isNotEmpty(interpretation.getSecondaryFindings())) {
            for (ReportedVariant secondaryFinding : interpretation.getSecondaryFindings()) {
                // Secondary node and relation: interpretation - secondary finding
                Node findingNode = loadReportedVariant(secondaryFinding);
                interpretationNode.createRelationshipTo(findingNode,
                        withName(SECONDARY_FINDING___INTERPRETATION___REPORTED_VARIANT.toString()));
            }
        }

        // Low coverage regions
        if (CollectionUtils.isNotEmpty(interpretation.getLowCoverageRegions())) {
            for (ReportedLowCoverage lowCoverageRegion : interpretation.getLowCoverageRegions()) {
                // Low coverage region node and relation: interpretation - low coverage region
                Node lowCoverageRegionNode = createNeo4JNode(NodeBuilder.newNode(0, lowCoverageRegion));
                interpretationNode.createRelationshipTo(lowCoverageRegionNode, withName(INTERPRETATION__LOW_COVERAGE_REGION.toString()));
            }
        }

        // Comments
        if (CollectionUtils.isNotEmpty(interpretation.getComments())) {
            for (Comment comment : interpretation.getComments()) {
                // Comment node and delation: interpretation - comment
                Node commentNode = createNeo4JNode(NodeBuilder.newNode(0, comment));
                interpretationNode.createRelationshipTo(commentNode, withName(INTERPRETATION__COMMENT.toString()));
            }
        }

        return interpretationNode;
    }

    private Node loadSoftware(Software software) {
        Node softwareNode = graphDb.findNode(Label.label(SOFTWARE.name()), "id", NodeBuilder.getSoftwareId(software));
        if (softwareNode != null) {
            return softwareNode;
        }

        return createNeo4JNode(NodeBuilder.newNode(0, software));
    }

    public Node loadReportedVariant(ReportedVariant reportedVariant) {
        Node reportedVariantNode = createNeo4JNode(NodeBuilder.newNode(0, reportedVariant));

        // Process variant and relation it to the reported variant
        Node variantNode = loadVariant(reportedVariant);
        reportedVariantNode.createRelationshipTo(variantNode, withName(REPORTED_VARIANT__VARIANT.toString()));

        //log.info("================> reported events for " + reportedVariant.toStringSimple() + " ? "
        //         + CollectionUtils.isNotEmpty(reportedVariant.getEvidences()));
        if (CollectionUtils.isNotEmpty(reportedVariant.getEvidences())) {
            //log.info(reportedVariant.getEvidences().size() + " reported event for reported variant " + reportedVariant.toStringSimple());
            for (ReportedEvent evidence : reportedVariant.getEvidences()) {
                // Comment node and delation: interpretation - comment
                Node reportedEventNode = loadReportedEvent(evidence);
                reportedVariantNode.createRelationshipTo(reportedEventNode, withName(REPORTED_VARIANT__REPORTED_EVENT.toString()));
            }
        }

        // Comments
        if (CollectionUtils.isNotEmpty(reportedVariant.getComments())) {
            for (Comment comment : reportedVariant.getComments()) {
                // Comment node and delation: interpretation - comment
                Node commentNode = createNeo4JNode(NodeBuilder.newNode(0, comment));
                reportedVariantNode.createRelationshipTo(commentNode, withName(REPORTED_VARIANT__COMMENT.toString()));
            }
        }

        return reportedVariantNode;
    }

    public Node loadReportedEvent(ReportedEvent reportedEvent) {
        Node reportedEventNode = createNeo4JNode(NodeBuilder.newNode(0, reportedEvent));

        // Phenotypes
        if (CollectionUtils.isNotEmpty(reportedEvent.getPhenotypes())) {
            for (Phenotype phenotype : reportedEvent.getPhenotypes()) {
                // Phenotype node and relation reported event - phenotype
                Node phenotypeNode = loadPhenotype(phenotype);
                reportedEventNode.createRelationshipTo(phenotypeNode, withName(REPORTED_EVENT__PHENOTYPE.toString()));
            }
        }

        // Sequence ontology terms (SO)
        if (CollectionUtils.isNotEmpty(reportedEvent.getConsequenceTypes())) {
            for (SequenceOntologyTerm so : reportedEvent.getConsequenceTypes()) {
                Node soNode = graphDb.findNode(Label.label(SO.name()), "id", so.getAccession());
                if (soNode == null) {
                    //log.info("SO " + so.getAccession() + ", " + so.getName() + " not found for reported event!");
                    soNode = createNeo4JNode(new org.opencb.bionetdb.core.models.network.Node(0, so.getAccession(),
                            so.getName(), org.opencb.bionetdb.core.models.network.Node.Type.SO));
                }
                reportedEventNode.createRelationshipTo(soNode, withName(REPORTED_EVENT__SO.toString()));
            }
        }

        // Genomic feature
        if (reportedEvent.getGenomicFeature() != null) {
            GenomicFeature genomicFeature = reportedEvent.getGenomicFeature();
            Node genomicFeatureNode = graphDb.findNode(Label.label(GENOMIC_FEATURE.name()), "id", genomicFeature.getId());
            if (genomicFeatureNode == null) {
                //log.info("Genomic feature " + genomicFeature.getId() + " not found for reported event!");
                genomicFeatureNode = createNeo4JNode(NodeBuilder.newNode(0, genomicFeature));
            }
            reportedEventNode.createRelationshipTo(genomicFeatureNode, withName(REPORTED_EVENT__GENOMIC_FEATURE.toString()));
        }

        // Panel
        if (StringUtils.isNotEmpty(reportedEvent.getPanelId())) {
            Node panelNode = graphDb.findNode(Label.label(PANEL.name()), "id", reportedEvent.getPanelId());
            if (panelNode == null) {
                //log.info("Panel " + reportedEvent.getPanelId() + " not found for reported event!");
                panelNode = createNeo4JNode(new org.opencb.bionetdb.core.models.network.Node(0, reportedEvent.getPanelId(),
                        "", PANEL));
            }
            reportedEventNode.createRelationshipTo(panelNode, withName(REPORTED_EVENT__PANEL.toString()));
        }

        return reportedEventNode;
    }

    public Node loadVariant(Variant variant) {
        Node variantNode = graphDb.findNode(Label.label("VARIANT"), "id", variant.toString());
        if (variantNode != null) {
            //log.info("Variant ID " + variant.toString() + " already loaded. Skip.");
            return variantNode;
        }

        variantNode = createNeo4JNode(NodeBuilder.newNode(0, variant));

        // Annotation management
        if (variant.getAnnotation() != null) {
            // Consequence types
            if (ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                // Consequence type nodes
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    // Consequence type node and relation variant - consequence type
                    Node ctNode = createNeo4JNode(NodeBuilder.newNode(0, ct));
                    variantNode.createRelationshipTo(ctNode, withName(VARIANT__CONSEQUENCE_TYPE.toString()));

                    // Transcript node and relation consequence type - transcript
                    if (ct.getEnsemblTranscriptId() != null) {
                        Node transcriptNode = graphDb.findNode(Label.label(TRANSCRIPT.name()), "id", ct.getEnsemblTranscriptId());
                        if (transcriptNode != null) {
                            ctNode.createRelationshipTo(transcriptNode, withName(CONSEQUENCE_TYPE__TRANSCRIPT.toString()));
                        } else {
                            log.warn("Transcript " + ct.getEnsemblTranscriptId() + " not found for gene " + ct.getEnsemblGeneId() + ", "
                                    + ct.getGeneName());
                        }
                    } else {
                        log.warn("Transcript null for gene " + ct.getEnsemblGeneId() + ", " + ct.getGeneName());
                    }

                    // SO
                    if (ListUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                        for (SequenceOntologyTerm so : ct.getSequenceOntologyTerms()) {
                            // SO node and relation consequence type - so
                            Node soNode = graphDb.findNode(Label.label(SO.toString()), "id", so.getAccession());
                            if (soNode == null) {
//                                log.info("SO term accession " + so.getAccession() + " not found.");
                                soNode = createNeo4JNode(new org.opencb.bionetdb.core.models.network.Node(0, so.getAccession(),
                                        so.getName(), org.opencb.bionetdb.core.models.network.Node.Type.SO));
                            }
                            ctNode.createRelationshipTo(soNode, withName(CONSEQUENCE_TYPE__SO.toString()));
                        }
                    }

                    // Protein variant annotation: substitution scores, keywords and features
                    if (ct.getProteinVariantAnnotation() != null) {
                        ProteinVariantAnnotation pVA = ct.getProteinVariantAnnotation();

                        // Protein variant annotation node and relation consequence type - protein variant annotation
                        Node pVANode = createNeo4JNode(NodeBuilder.newNode(0, pVA));
                        ctNode.createRelationshipTo(pVANode, withName(CONSEQUENCE_TYPE__PROTEIN_VARIANT_ANNOTATION.toString()));

                        // Protein relationship management
                        if (pVA.getUniprotAccession() != null) {
                            Node proteinNode = graphDb.findNode(Label.label(PROTEIN.name()), "id", pVA.getUniprotAccession());
                            if (proteinNode != null) {
                                pVANode.createRelationshipTo(proteinNode, withName(PROTEIN_VARIANT_ANNOTATION__PROTEIN.toString()));
                            } else {
                                log.warn("Protein " + pVA.getUniprotAccession() + " node not found for protein variant annotation ("
                                        + ct.getEnsemblGeneId() + ", " + ct.getGeneName() + ", " + ct.getEnsemblTranscriptId() + ")");
                            }
                        } else {
                            log.warn("Protein Uniprot accession null for protein variant annotation (" + ct.getEnsemblGeneId() + ", "
                                    + ct.getGeneName() + ", " + ct.getEnsemblTranscriptId() + ")");
                        }

                        // Protein substitution scores
                        if (ListUtils.isNotEmpty(ct.getProteinVariantAnnotation().getSubstitutionScores())) {
                            for (Score score: ct.getProteinVariantAnnotation().getSubstitutionScores()) {
                                Node scoreNode = createNeo4JNode(NodeBuilder.newNode(0, score,
                                        org.opencb.bionetdb.core.models.network.Node.Type.SUBSTITUTION_SCORE));
                                pVANode.createRelationshipTo(scoreNode,
                                        withName(PROTEIN_VARIANT_ANNOTATION__SUBSTITUTION_SCORE.toString()));
                            }
                        }
                    }
                }
            }

            // Population frequencies
            if (ListUtils.isNotEmpty(variant.getAnnotation().getPopulationFrequencies())) {
                for (PopulationFrequency popFreq : variant.getAnnotation().getPopulationFrequencies()) {
                    // Population frequency node and relation: variant - population frequency
                    Node popFreqNode = createNeo4JNode(NodeBuilder.newNode(0, popFreq));
                    variantNode.createRelationshipTo(popFreqNode, withName(VARIANT__POPULATION_FREQUENCY.toString()));
                }
            }

            // Conservation values
            if (ListUtils.isNotEmpty(variant.getAnnotation().getConservation())) {
                for (Score score: variant.getAnnotation().getConservation()) {
                    // Conservation node and relation: variant - conservation
                    Node conservatioNode = createNeo4JNode(NodeBuilder.newNode(0, score, CONSERVATION));
                    variantNode.createRelationshipTo(conservatioNode, withName(VARIANT__CONSERVATION.toString()));
                }
            }

            // Trait associations
            if (ListUtils.isNotEmpty(variant.getAnnotation().getTraitAssociation())) {
                for (EvidenceEntry evidence: variant.getAnnotation().getTraitAssociation()) {
                    // Trait association node and relation: variant - trait association
                    Node traitNode = createNeo4JNode(NodeBuilder.newNode(0, evidence, TRAIT_ASSOCIATION));
                    variantNode.createRelationshipTo(traitNode, withName(VARIANT__TRAIT_ASSOCIATION.toString()));
                }
            }

            // Functional scores
            if (ListUtils.isNotEmpty(variant.getAnnotation().getFunctionalScore())) {
                for (Score score: variant.getAnnotation().getFunctionalScore()) {
                    // Functional score node and relation: variant - functional score
                    Node functNode = createNeo4JNode(NodeBuilder.newNode(0, score, FUNCTIONAL_SCORE));
                    variantNode.createRelationshipTo(functNode, withName(VARIANT__FUNCTIONAL_SCORE.toString()));
                }
            }
        }

        return variantNode;
    }

    public Node loadFile(File file) {
        Node fileNode = graphDb.findNode(Label.label(FILE.name()), "id", file.getId());
        if (fileNode != null) {
            return fileNode;
        }

        fileNode = createNeo4JNode(NodeBuilder.newNode(0, file));

        // Software
        if (file.getSoftware() != null) {
            Node softwareNode = loadSoftware(file.getSoftware());
            fileNode.createRelationshipTo(softwareNode, withName(FILE__SOFTWARE.toString()));
        }

        // Experiment
        if (file.getExperiment() != null) {
            Node experimentNode = createNeo4JNode(NodeBuilder.newNode(0, file.getExperiment()));
            fileNode.createRelationshipTo(experimentNode, withName(FILE__EXPERIMENT.toString()));
        }

        // Samples
        if (CollectionUtils.isNotEmpty(file.getSamples())) {
            for (Sample sample : file.getSamples()) {
                Node sampleNode = loadSample(sample);
                fileNode.createRelationshipTo(sampleNode, withName(FILE__SAMPLE.toString()));
            }
        }

        return fileNode;
    }

    public Node loadFamily(Family family) {
        Node familyNode = graphDb.findNode(Label.label(FAMILY.name()), "id", family.getId());
        if (familyNode != null) {
            return familyNode;
        }

        //log.info("Family " + family.getId() + ", " + family.getName() + " not found. Create it!");
        familyNode = createNeo4JNode(NodeBuilder.newNode(0, family));

        // Phenotypes
        //log.info("Family: loading phenotypes...");
        if (CollectionUtils.isNotEmpty(family.getPhenotypes())) {
            for (Phenotype phenotype : family.getPhenotypes()) {
                Node phenotypeNode = loadPhenotype(phenotype);
                familyNode.createRelationshipTo(phenotypeNode, withName(FAMILY__PHENOTYPE.toString()));
            }
        }

        // Disorders
        //log.info("Family: loading disorders...");
        if (CollectionUtils.isNotEmpty(family.getDisorders())) {
            for (Disorder disorder : family.getDisorders()) {
                Node disorderNode = loadDisorder(disorder);
                familyNode.createRelationshipTo(disorderNode, withName(FAMILY__DISORDER.toString()));
            }
        }

        // Members
        //log.info("Family: loading members...");
        if (CollectionUtils.isNotEmpty(family.getMembers())) {
            for (Individual member : family.getMembers()) {
                Node memberNode = loadIndividual(member);
                familyNode.createRelationshipTo(memberNode, withName(FAMILY__INDIVIDUAL.toString()));
            }
        }

        return familyNode;
    }

    public Node loadIndividual(Individual individual) {
        Node individualNode = graphDb.findNode(Label.label(INDIVIDUAL.name()), "id", individual.getId());

        if (individualNode != null) {
            return individualNode;
        }

        //log.info("Individual " + individual.getId() + ", " + individual.getName() + " not found. Create it!");
        individualNode = createNeo4JNode(NodeBuilder.newNode(0, individual));

        // Father
        if (individual.getFather() != null) {
            Node fatherNode = loadIndividual(individual.getFather());
            fatherNode.createRelationshipTo(individualNode, withName(FATHER_OF___INDIVIDUAL___INDIVIDUAL.toString()));
        }

        // Mother
        if (individual.getMother() != null) {
            Node motherNode = loadIndividual(individual.getMother());
            motherNode.createRelationshipTo(individualNode, withName(MOTHER_OF___INDIVIDUAL___INDIVIDUAL.toString()));
        }

        // Phenotypes
        if (CollectionUtils.isNotEmpty(individual.getPhenotypes())) {
            for (Phenotype phenotype : individual.getPhenotypes()) {
                Node phenotypeNode = loadPhenotype(phenotype);
                individualNode.createRelationshipTo(phenotypeNode, withName(INDIVIDUAL__PHENOTYPE.toString()));
            }
        }

        // Disorders
        if (CollectionUtils.isNotEmpty(individual.getDisorders())) {
            for (Disorder disorder : individual.getDisorders()) {
                Node disorderNode = loadDisorder(disorder);
                individualNode.createRelationshipTo(disorderNode, withName(INDIVIDUAL__DISORDER.toString()));
            }
        }

        // Samples
        if (CollectionUtils.isNotEmpty(individual.getSamples())) {
            for (Sample sample : individual.getSamples()) {
                Node sampleNode = loadSample(sample);
                individualNode.createRelationshipTo(sampleNode, withName(INDIVIDUAL__SAMPLE.toString()));
            }
        }
        return individualNode;
    }

    public Node loadDisorder(Disorder disorder) {
        // Disorder node
        Node disorderNode = createNeo4JNode(NodeBuilder.newNode(0, disorder));
        if (CollectionUtils.isNotEmpty(disorder.getEvidences())) {
            for (Phenotype phenotype : disorder.getEvidences()) {
                // Phenotype node and relation: disorder - phenotype
                Node phenotypeNode = loadPhenotype(phenotype);
                disorderNode.createRelationshipTo(phenotypeNode, withName(DISORDER__PHENOTYPE.toString()));
            }
        }
        return disorderNode;
    }

    public Node loadPhenotype(Phenotype phenotype) {
        //log.info("Loading Phenotype, id = " + phenotype.getId() + ", name = " + phenotype.getName());
        Node phenotypeNode = createNeo4JNode(NodeBuilder.newNode(0, phenotype));

        Node ontologyTermNode = loadOntologyTerm(phenotype);
        phenotypeNode.createRelationshipTo(ontologyTermNode, withName(PHENOTYPE__ONTOLOGY_TERM.name()));
        //log.info("Done. Phenotype, id = " + phenotype.getId() + ", name = " + phenotype.getName());
        return phenotypeNode;
    }

    public Node loadOntologyTerm(OntologyTerm ontologyTerm) {
        //log.info("Loading OntologyTerm, id = " + ontologyTerm.getId() + ", name = " + ontologyTerm.getName());
        Node ontologyTermNode = graphDb.findNode(Label.label(ONTOLOGY_TERM.name()), "id", ontologyTerm.getId());
        if (ontologyTermNode == null) {
            ontologyTermNode = createNeo4JNode(NodeBuilder.newNode(0, ontologyTerm));
        }
        //log.info("Done. OntologyTerm, id = " + ontologyTerm.getId() + ", name = " + ontologyTerm.getName());
        return ontologyTermNode;
    }

    public Node loadSample(Sample sample) {
        Node sampleNode = graphDb.findNode(Label.label(SAMPLE.name()), "id", sample.getId());
        if (sampleNode != null) {
            return sampleNode;
        }

        //log.info("Sample " + sample.getId() + ", " + sample.getName() + " not found. Create it!");
        sampleNode = createNeo4JNode(NodeBuilder.newNode(0, sample));

        if (CollectionUtils.isNotEmpty(sample.getPhenotypes())) {
            for (Phenotype phenotype : sample.getPhenotypes()) {
                // Phenotype node and relation sample - phenotype
                Node phenotypeNode = loadPhenotype(phenotype);
                sampleNode.createRelationshipTo(phenotypeNode, withName(SAMPLE__PHENOTYPE.name()));
            }
        }
        return sampleNode;
    }

    public Node loadPanel(DiseasePanel panel) {
        Node panelNode = graphDb.findNode(Label.label(PANEL.name()), "id", panel.getId());
        if (panelNode != null) {
            return panelNode;
        }

        //log.info("Panel " + panel.getId() + ", " + panel.getName() + " not found. Create it!");
        panelNode = createNeo4JNode(NodeBuilder.newNode(0, panel));
        // Phenotypes
        if (CollectionUtils.isNotEmpty(panel.getPhenotypes())) {
            for (Phenotype phenotype : panel.getPhenotypes()) {
                // Phenotype node and relation panel - phenotype
                Node phenotypeNode = loadPhenotype(phenotype);
                panelNode.createRelationshipTo(phenotypeNode, withName(PANEL__PHENOTYPE.name()));
            }
        }
        // Panel variants (DiseasePanel.VariantPanel)
        if (CollectionUtils.isNotEmpty(panel.getVariants())) {
            for (DiseasePanel.VariantPanel panelVariant : panel.getVariants()) {
                Node panelVariantNode = createNeo4JNode(NodeBuilder.newNode(0, panelVariant));
                panelNode.createRelationshipTo(panelVariantNode, withName(PANEL__PANEL_VARIANT.name()));
                addOntologyTerms(panelVariant.getPhenotypes(), panelVariantNode, withName(PANEL_VARIANT__ONTOLOGY_TERM.name()));

                Node variantNode = graphDb.findNode(Label.label(VARIANT.name()), "id", panelVariant.getId());
                if (variantNode != null) {
                    panelVariantNode.createRelationshipTo(variantNode, withName(PANEL_VARIANT__VARIANT.name()));
                } else {
                    log.warn("Variant not found for panel variant " + panelVariant.getId());
                }
            }
        }
        // Panel genes (DiseasePanel.GenePanel)
        if (CollectionUtils.isNotEmpty(panel.getGenes())) {
            for (DiseasePanel.GenePanel panelGene : panel.getGenes()) {
                Node panelGeneNode = createNeo4JNode(NodeBuilder.newNode(0, panelGene));
                panelNode.createRelationshipTo(panelGeneNode, withName(PANEL__PANEL_GENE.name()));
                addOntologyTerms(panelGene.getPhenotypes(), panelGeneNode, withName(PANEL_GENE__ONTOLOGY_TERM.name()));

                Node geneNode = graphDb.findNode(Label.label(GENE.name()), "id", panelGene.getId());
                if (geneNode != null) {
                    panelGeneNode.createRelationshipTo(geneNode, withName(PANEL_GENE__GENE.name()));
                } else {
                    log.warn("Gene not found for panel gene " + panelGene.getId() + ", " + panelGene.getName());
                }
            }
        }
        // STRs (DiseasePanel.STR)
        if (CollectionUtils.isNotEmpty(panel.getStrs())) {
            for (DiseasePanel.STR panelStr : panel.getStrs()) {
                Node panelStrNode = createNeo4JNode(NodeBuilder.newNode(0, panelStr));
                panelNode.createRelationshipTo(panelStrNode, withName(PANEL__PANEL_STR.name()));
                addOntologyTerms(panelStr.getPhenotypes(), panelStrNode, withName(PANEL_STR__ONTOLOGY_TERM.name()));
            }
        }
        // Panel regions (DiseasePanel.RegionPanel)
        if (CollectionUtils.isNotEmpty(panel.getRegions())) {
            for (DiseasePanel.RegionPanel panelRegion : panel.getRegions()) {
                Node panelRegionNode = createNeo4JNode(NodeBuilder.newNode(0, panelRegion));
                panelNode.createRelationshipTo(panelRegionNode, withName(PANEL__PANEL_REGION.name()));
                addOntologyTerms(panelRegion.getPhenotypes(), panelRegionNode, withName(PANEL_STR__ONTOLOGY_TERM.name()));
            }
        }
        return panelNode;
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private void addOntologyTerms(List<OntologyTerm> ontologyTerms, Node node, RelationshipType relation) {
        if (CollectionUtils.isNotEmpty(ontologyTerms)) {
            for (OntologyTerm ontologyTerm : ontologyTerms) {
                Node ontologyTermNode = loadOntologyTerm(ontologyTerm);
                node.createRelationshipTo(ontologyTermNode, relation);
            }
        }
    }

    private Node createNeo4JNode(org.opencb.bionetdb.core.models.network.Node node) {
        Node neo4jNode = graphDb.createNode(Label.label(node.getType().toString()));

        neo4jNode.setProperty("uid", neo4jNode.getId());

        if (StringUtils.isNotEmpty(node.getId())) {
            neo4jNode.setProperty("id", node.getId());
        }

        if (StringUtils.isNotEmpty(node.getName())) {
            neo4jNode.setProperty("name", node.getName());
        }

        if (StringUtils.isNotEmpty(node.getSource())) {
            neo4jNode.setProperty("source", node.getSource());
        }

        if (MapUtils.isNotEmpty(node.getAttributes())) {
            for (Map.Entry<String, Object> entry : node.getAttributes().entrySet()) {
                neo4jNode.setProperty(entry.getKey(), entry.getValue());
            }
        }
        return neo4jNode;
    }
}
