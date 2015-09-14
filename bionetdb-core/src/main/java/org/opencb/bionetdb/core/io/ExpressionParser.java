package org.opencb.bionetdb.core.io;

import org.opencb.bionetdb.core.models.Expression;
import org.opencb.commons.utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 8/9/15.
 */
public class ExpressionParser {

    private Map<String, Map<String, String>> myFiles;

    public ExpressionParser(Path metadata) throws IOException {
        FileUtils.checkFile(metadata);

        myFiles = new HashMap<>();
        List<String> allLines = Files.readAllLines(metadata);
        for (String line : allLines) {
            String[] fields = line.split("\t");
            Map<String, String> timeSeries;
            if (myFiles.containsKey(fields[0])) {
                timeSeries = myFiles.get(fields[0]);
            } else {
                timeSeries = new HashMap<>();
            }
            timeSeries.put(fields[1], metadata.getParent().toString() + "/" + fields[2]);
            if (!myFiles.containsKey(fields[0]))
                myFiles.put(fields[0], timeSeries);
        }
    }

    public List<Expression> parse(String tissue, String time) throws IOException {
        String expressionFile = myFiles.get(tissue).get(time);
        List<Expression> myExpressionList = new ArrayList<>();

        int colId = -1;
        int colExpr = -1;
        int colPval = -1;
        int colOdds = -1;
        int colUpreg = -1;

        // Open expression file, this can be gzipped
        Path expressionFilePath = Paths.get(expressionFile);
        BufferedReader br = FileUtils.newBufferedReader(expressionFilePath);

        // Finding the index position for each column
        String currentLine = br.readLine();
        String[] headers = currentLine.split("\t", -1);
        if (headers.length > 1) {
            // Get the column where we can find each of the columns
            for (int i = 0; i < headers.length; i++) {
                switch(headers[i]) {
                    case "ID":
                        colId = i;
                        break;
                    case "Expression":
                        colExpr = i;
                        break;
                    case "Pvalue":
                        colPval = i;
                        break;
                    case "Odds":
                        colOdds = i;
                        break;
                    case "Upregulated":
                        colUpreg = i;
                        break;
                    default:
                        break;
                }
            }

            // It we have the geneID at least and some expression, pvalues, odds or upregulations...
            if (colId != -1 && (colExpr != -1 ||colOdds != -1 ||colPval != -1 ||colUpreg != -1)) {
                while ((currentLine = br.readLine()) != null) {
                    String[] line_spl = currentLine.split("\t", -1);
                    Expression myExpr = new Expression(line_spl[colId]);
                    if (colExpr != -1) {
                        myExpr.setExpression(Double.parseDouble(line_spl[colExpr]));
                    }
                    if (colOdds != -1) {
                        myExpr.setOdds(Double.parseDouble(line_spl[colOdds]));
                    }
                    if (colPval != -1) {
                        myExpr.setPvalue(Double.parseDouble(line_spl[colPval]));
                    }
                    if (colUpreg != -1) {
                        myExpr.setUpregulated(Integer.parseInt(line_spl[colUpreg]));
                    }
                    myExpressionList.add(myExpr);
                }
            }
        }

        return myExpressionList;
    }

    @Override
    public String toString() {
        return "ExpressionParser{" +
                "myFiles=" + myFiles +
                '}';
    }

    public Map<String, Map<String, String>> getMyFiles() {
        return myFiles;
    }

    public void setMyFiles(Map<String, Map<String, String>> myFiles) {
        this.myFiles = myFiles;
    }

}
