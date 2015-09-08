package org.opencb.bionetdb.core.io;

import org.opencb.bionetdb.core.models.Expression;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.readAllLines;

/**
 * Created by pfurio on 8/9/15.
 */
public class ExpressionParser {

    private Map<String, Map<String, String>> myFiles;

    public ExpressionParser(Path metadata) throws IOException {
        myFiles = new HashMap<>();
        List<String> allLines = readAllLines(metadata);
        Map<String, String> timeseries = new HashMap<>();
        for (String line : allLines) {
            String[] fields = line.split("\t");
            timeseries.put(fields[1], fields[2]);
            this.myFiles.put(fields[0], timeseries);
            timeseries.clear();
        }
    }

    public Map<String, Map<String, String>> getMyFiles() {
        return myFiles;
    }

    public List<Expression> parse(String tissue, String time) throws IOException {

        String expressionFile = myFiles.get(tissue).get(time);
        List<Expression> myExpressionList = new ArrayList<>();

        // Open expression file
        BufferedReader br = new BufferedReader(new FileReader(expressionFile));
        String currentLine = br.readLine();

        int col_id, col_expr, col_pval, col_odds, col_upreg;
        col_id = col_expr = col_pval = col_odds = col_upreg = -1;
        String[] headers = currentLine.split("\t", -1);
        if (headers.length == 2 || headers.length == 5) {

            // Get the column where we can find each of the columns
            for (int i = 0; i < headers.length; i++) {
                switch(headers[i]) {
                    case "ID":
                        col_id = i;
                        break;
                    case "Expression":
                        col_expr = i;
                        break;
                    case "Pvalue":
                        col_pval = i;
                        break;
                    case "Odds":
                        col_odds = i;
                        break;
                    case "Upregulated":
                        col_upreg = i;
                        break;
                    default:
                        break;
                }
            }

            // It we have the geneID at least and some expression, pvalues, odds or upregulations...
            if (col_id != -1 && (col_expr != -1 ||col_odds != -1 ||col_pval != -1 ||col_upreg != -1)) {
                while ((currentLine = br.readLine()) != null) {
                    String[] line_spl = currentLine.split("\t", -1);
                    Expression myexpr = new Expression(line_spl[col_id]);
                    if (col_expr != -1)
                        myexpr.setExpression(Double.parseDouble(line_spl[col_expr]));
                    if (col_odds != -1)
                        myexpr.setOdds(Double.parseDouble(line_spl[col_odds]));
                    if (col_pval != -1)
                        myexpr.setPvalue(Double.parseDouble(line_spl[col_pval]));
                    if (col_upreg != -1)
                        myexpr.setUpregulated(Integer.parseInt(line_spl[col_upreg]));
                    myExpressionList.add(myexpr);
                }
            }
        }

        return myExpressionList;
    }

}
