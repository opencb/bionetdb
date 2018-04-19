package org.opencb.bionetdb.core.utils;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.VcfFileReader;
import org.opencb.biodata.tools.variant.converters.avro.VariantContextToVariantConverter;
import org.opencb.commons.utils.ListUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

public class Neo4JImporter {

    private final int VARIANT_BATCH_SIZE = 200;
    private CSVForVCF csvForVCF;

    private class CSVForVCF {
        public String variantCSV;
        public String variantcallCSV;
        public String variantfileinfoCSV;
        public String sampleCSV;
        public String variant_variantcallCSV;
        public String sample_variantcallCSV;
        public String variantcall_variantfileinfoCSV;

        public PrintWriter variantPW;
        public PrintWriter variantcallPW;
        public PrintWriter variantfileinfoPW;
        public PrintWriter samplePW;
        public PrintWriter variant_variantcallPW;
        public PrintWriter sample_variantcallPW;
        public PrintWriter variantcall_variantfileinfoPW;

        public Set<String> formatSet;
        public Set<String> infoSet;

        private Path vcfPath;
        private Path outPath;
        private VCFHeader vcfHeader;

        public CSVForVCF(Path vcfPath) throws FileNotFoundException {
            this.vcfPath = vcfPath;

            String name = vcfPath.toFile().getName();
            variantCSV = name + ".variant.cvs";
            variantcallCSV = name + ".variantcall.cvs";
            variantfileinfoCSV = name + ".variantfileinfo.cvs";
            sampleCSV = name + ".sample.cvs";
            variant_variantcallCSV = name + ".variant_variantcall.cvs";
            sample_variantcallCSV = name + ".sample_variantcall.cvs";
            variantcall_variantfileinfoCSV = name + ".variantcall_variantfileinfo.cvs";

            formatSet = new LinkedHashSet<>();
            infoSet = new LinkedHashSet<>();
        }

        public void open(Path outPath) throws FileNotFoundException {
            this.outPath = outPath;

            // Node files
            variantPW = new PrintWriter(outPath + "/" + variantCSV);
            variantcallPW = new PrintWriter(outPath + "/" + variantcallCSV);
            variantfileinfoPW = new PrintWriter(outPath + "/" + variantfileinfoCSV);
            samplePW = new PrintWriter(outPath + "/" + sampleCSV);

            // Relationship files
            variant_variantcallPW = new PrintWriter(outPath + "/" + variant_variantcallCSV);
            sample_variantcallPW = new PrintWriter(outPath + "/" + sample_variantcallCSV);
            variantcall_variantfileinfoPW = new PrintWriter(outPath + "/" + variantcall_variantfileinfoCSV);
        }

        public void writeHeaders(VCFHeader vcfHeader) {
            this.vcfHeader = vcfHeader;

            StringBuilder sb = new StringBuilder();

            // VARIANT nodes
            variantPW.println("name:ID(variantId),id,attr_chromosome,attr_start:INT,attr_end:INT,attr_strand,attr_reference,attr_alternate,"
                    + "attr_type");

            // SAMPLE nodes
            samplePW.println("id:ID(sampleId),name");

            // VARIANT_CALL nodes
            Collection<VCFFormatHeaderLine> formatHeaderLines = vcfHeader.getFormatHeaderLines();
            sb.setLength(0);
            for (VCFFormatHeaderLine formatHeaderLine: vcfHeader.getFormatHeaderLines()) {
                formatSet.add(formatHeaderLine.getID());
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append("attr_").append(formatHeaderLine.getID());
            }
            variantcallPW.print("id:ID(variantCallId),name");
            if (sb.length() > 0) {
                variantcallPW.print(",");
                variantcallPW.println(sb.toString());
            } else {
                variantcallPW.println("");
            }

            // VARIANT_FILE_INFO nodes
            Collection<VCFInfoHeaderLine> infoHeaderLines = vcfHeader.getInfoHeaderLines();
            sb.setLength(0);
            for (VCFInfoHeaderLine infoHeaderLine: vcfHeader.getInfoHeaderLines()) {
                infoSet.add(infoHeaderLine.getID());
                if (sb.length() > 0) {
                    sb.append(",");
                }
                sb.append("attr_").append(infoHeaderLine.getID());
            }
            variantfileinfoPW.print("id:ID(variantFileInfoId),name,attr_filename");
            if (sb.length() > 0) {
                variantfileinfoPW.print(",");
                variantfileinfoPW.println(sb.toString());
            } else {
                variantfileinfoPW.println("");
            }

            // VARIANT -> VARIANT_CALL relationship
            variant_variantcallPW.println(":START_ID(variantId),:END_ID(variantCallId)");

            // SAMPLE -> VARIANT_CALL relationship
            sample_variantcallPW.println(":START_ID(sampleId),:END_ID(variantCallId)");

            // VARIANT_CALL -> VARIANT_FILE_INFO relationship
            variantcall_variantfileinfoPW.println(":START_ID(variantCallId),:END_ID(variantFileInfoId)");
        }

        public void close() {
            variantPW.close();
            variantcallPW.close();
            variantfileinfoPW.close();
            samplePW.close();
            variant_variantcallPW.close();
            sample_variantcallPW.close();
            variantcall_variantfileinfoPW.close();
        }
    }

    public void generateCSVFromVCF(Path vcfPath, Path outDir) throws FileNotFoundException {
        this.csvForVCF = new CSVForVCF(vcfPath);

        // VCF File reader management
        VcfFileReader vcfFileReader = new VcfFileReader(vcfPath.toString(), false);
        vcfFileReader.open();
        VCFHeader vcfHeader = vcfFileReader.getVcfHeader();

        csvForVCF.open(outDir);
        csvForVCF.writeHeaders(vcfHeader);

        // sample.cvs for SAMPLE nodes
        List<String> sampleNames = vcfHeader.getSampleNamesInOrder();
        csvForVCF.samplePW.println("id:ID(sampleId),name");
        for (String sampleName: sampleNames) {
            csvForVCF.samplePW.println(sampleName + "," + sampleName);
        }
        csvForVCF.samplePW.close();

        // VariantContext-to-Variant converter
        VariantContextToVariantConverter converter = new VariantContextToVariantConverter("dataset", vcfPath.toFile().getName(),
                vcfFileReader.getVcfHeader().getSampleNamesInOrder());

        List<VariantContext> variantContexts = vcfFileReader.read(VARIANT_BATCH_SIZE);
        while (variantContexts.size() == VARIANT_BATCH_SIZE) {
            updateCSVVariantFiles(Neo4JConverter.convert(variantContexts, converter), csvForVCF);

            // Read next batch
            variantContexts = vcfFileReader.read(VARIANT_BATCH_SIZE);
        }
        if (variantContexts.size() > 0) {
            updateCSVVariantFiles(Neo4JConverter.convert(variantContexts, converter), csvForVCF);
        }

        // Close CSV files
        csvForVCF.close();

        // close VCF file reader
        vcfFileReader.close();

    }

    public void importCSV(Path neo4jHome) throws IOException, InterruptedException {
        String home = neo4jHome == null ? System.getenv("NEO4J_HOME") : neo4jHome.toString();
        if (home == null) {
            throw new InterruptedException("Neo4j's home directory is missing. To fix it, please, set the environment "
                    + "variable NEO4J_HOME.");
        }

        String neo4j = home + "/bin/neo4j";
        String neo4jAdmin = home + "/bin/neo4j-admin";

        Runtime rt = Runtime.getRuntime();
        System.out.println("Stopping Neo4j...");
        Process pr = rt.exec(neo4j + " stop; rm -rf " + home + "/data/databases/graph.db");
        pr.waitFor();
        System.out.println("\t...done!");

        StringBuilder sb = new StringBuilder();
        sb.append(neo4jAdmin).append(" import")
                .append(" --nodes:SAMPLE ").append(csvForVCF.outPath).append("/").append(csvForVCF.sampleCSV)
                .append(" --nodes:VARIANT ").append(csvForVCF.outPath).append("/").append(csvForVCF.variantCSV)
                .append(" --nodes:VARIANT_CALL ").append(csvForVCF.outPath).append("/").append(csvForVCF.variantcallCSV)
                .append(" --nodes:VARIANT_FILE_INFO ").append(csvForVCF.outPath).append("/").append(csvForVCF.variantfileinfoCSV)
                .append(" --relationships:VARIANT_CALL ").append(csvForVCF.outPath).append("/").append(csvForVCF.variant_variantcallCSV)
                .append(" --relationships:VARIANT_CALL ").append(csvForVCF.outPath).append("/").append(csvForVCF.sample_variantcallCSV)
                .append(" --relationships:VARIANT_FILE_INFO ").append(csvForVCF.outPath).append("/")
                .append(csvForVCF.variantcall_variantfileinfoCSV);

        rt = Runtime.getRuntime();
        System.out.println("Executing:\n\t" + sb.toString());
        pr = rt.exec(sb.toString());

        System.out.println(">>>>> STD ERROR");
        BufferedReader in = new BufferedReader(new InputStreamReader(pr.getErrorStream()));
        String line;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }

        System.out.println(">>>>> STD OUTPUT");
        in = new BufferedReader(new InputStreamReader(pr.getInputStream()));
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }

        pr.waitFor();
        System.out.println("\t...done!");

        rt = Runtime.getRuntime();
        System.out.println("Starting Neo4j...");
        pr = rt.exec(neo4j + " start");
        pr.waitFor();
        System.out.println("\t...done!");

        //./neo4j stop; rm -rf ../data/databases/graph.db ; ./neo4j-admin import
        // --nodes:SAMPLE /tmp/3.vcf.sample.cvs
        // --nodes:VARIANT /tmp/3.vcf.variant.cvs
        // --nodes:VARIANT_CALL /tmp/3.vcf.variantcall.cvs
        // --nodes:VARIANT_FILE_INFO /tmp/3.vcf.variantfileinfo.cvs
        // --relationships:VARIANT_CALL /tmp/3.vcf.variant_variantcall.cvs
        // --relationships:VARIANT_CALL /tmp/3.vcf.sample_variantcall.cvs
        // --relationships:VARIANT_FILE_INFO /tmp/3.vcf.variantcall_variantfileinfo.cvs  -h ; ./neo4j start

    }

    private void updateCSVVariantFiles(List<Variant> variants, CSVForVCF csv) {
        StringBuilder sb = new StringBuilder();
        List<String> sampleNames = csv.vcfHeader.getSampleNamesInOrder();

        for (Variant variant: variants) {

            // Variant management
            sb.setLength(0);
            sb.append(variant.toString()).append(",").append(variant.getId()).append(",").append(variant.getChromosome()).append(",")
                    .append(variant.getStart()).append(",").append(variant.getEnd()).append(",").append(variant.getStrand()).append(",")
                    .append(variant.getReference()).append(",").append(variant.getAlternate()).append(",").append(variant.getType());
            csv.variantPW.println(sb.toString());

            if (ListUtils.isNotEmpty(variant.getStudies())) {
                // Only one single study is supported
                StudyEntry studyEntry = variant.getStudies().get(0);

                if (ListUtils.isNotEmpty(studyEntry.getFiles())) {
                    // INFO management: FILTER, QUAL and info fields
                    String filename = studyEntry.getFiles().get(0).getFileId();
                    String infoId = variant.toString() + "_" + filename;
                    Map<String, String> fileAttrs = studyEntry.getFiles().get(0).getAttributes();

                    sb.setLength(0);
                    Iterator<String> iterator = csv.infoSet.iterator();
                    while (iterator.hasNext()) {
                        String infoName = iterator.next();
                        if (sb.length() > 0) {
                            sb.append(",");
                        }
                        if (fileAttrs.containsKey(infoName)) {
                            sb.append(fileAttrs.get(infoName).replace(",", ";"));
                        } else {
                            sb.append("-");
                        }
                    }
                    csv.variantfileinfoPW.print(infoId + "," + infoId + "," + filename);
                    if (sb.length() > 0) {
                        csv.variantfileinfoPW.print(",");
                        csv.variantfileinfoPW.println(sb.toString());
                    } else {
                        csv.variantfileinfoPW.println();
                    }

                    // FORMAT: GT and format attributes
                    for (int i = 0; i < studyEntry.getSamplesData().size(); i++) {
                        sb.setLength(0);
                        String formatId = variant.toString() + "_" + sampleNames.get(i);

                        sb.setLength(0);
                        iterator = csv.formatSet.iterator();
                        while (iterator.hasNext()) {
                            String formatName = iterator.next();
                            if (sb.length() > 0) {
                                sb.append(",");
                            }
                            if (studyEntry.getFormatPositions().containsKey(formatName)) {
                                sb.append(studyEntry.getSampleData(i).get(studyEntry.getFormatPositions().get(formatName))
                                        .replace(",", ";"));
                            } else {
                                sb.append("-");
                            }
                        }
                        csv.variantcallPW.print(formatId + "," + formatId);
                        if (sb.length() > 0) {
                            csv.variantcallPW.print(",");
                            csv.variantcallPW.println(sb.toString());
                        } else {
                            csv.variantcallPW.println();
                        }

                        // Relation: variant - variant call
                        sb.setLength(0);
                        sb.append(variant.toString()).append(",").append(formatId);
                        csv.variant_variantcallPW.println(sb.toString());

                        // Relation: sample - variant call
                        sb.setLength(0);
                        sb.append(sampleNames.get(i)).append(",").append(formatId);
                        csv.sample_variantcallPW.println(sb.toString());

                        // Relation: variant call - variant file info
                        sb.setLength(0);
                        sb.append(formatId).append(",").append(infoId);
                        csv.variantcall_variantfileinfoPW.println(sb.toString());
                    }
                }
            }
        }
    }
}
