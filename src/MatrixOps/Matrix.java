/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package MatrixOps;

import Modeling.ModelTypes;
import java.io.BufferedReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import StockData.StockDataHandler;

/**
 *
 * @author Matt Jones
 */
public class Matrix {

    //Output - y column is always the last element in the row
    public static MatrixValues loadMatrixFromDB(final String STOCK_TICKER, final int DAYS_IN_FUTURE, ModelTypes approach) throws Exception {

        //Pull the data from the DB
        StockDataHandler sdh = new StockDataHandler();
        List<double[]> stockData = sdh.getAllStockFeaturesFromDB(STOCK_TICKER, DAYS_IN_FUTURE, approach);

        //Now load the Lists with data
        List<double[]> rowList = new ArrayList<>();
        List<Double> outputList = new ArrayList<>();

        //Process the values - first element is x0 = 1, last element is y
        double[] featuresRow;
        int rowWidth = stockData.get(0).length - 1;
        for (double[] row : stockData) {

            //Parse out the features from the output value
            featuresRow = new double[rowWidth];
            for (int j = 0; j < row.length; j++) {
        
                //Move the output column y to the outputList
                if (j == row.length - 1)
                    outputList.add(row[j]);
                else
                    featuresRow[j] = row[j];
            }

            rowList.add(featuresRow);
        }
        
        //Now generate the Matrix and resultValues
        if (rowList.size() != outputList.size())
            throw new Exception("Row Mismatch in loadMatrixFromDB Method!");

        //Create the arrays
        double[][] matrix = new double[rowList.size()][rowWidth];
        double[] resultValues = new double[outputList.size()];

        //Copy Lists into arrays
        for (int i = 0; i < rowList.size(); i++) {
            matrix[i] = (double[])rowList.get(i);
            resultValues[i] = (double)outputList.get(i);
        }

        MatrixValues mv = null;
        mv = new MatrixValues(matrix, resultValues);

        System.out.println("Method: LoadMatrixFromDB, Ticker: " + STOCK_TICKER + ", Row Count = " + rowList.size());
        
        return mv;
    }
    
    public static MatrixValues loadMatrixFromFile(final String INPUT_FILE, final String DELIM, final boolean HEADER_ROW, final int NUM_FEATURES, final int OUTPUT_COLUMN) {
        
        Path p = Paths.get(INPUT_FILE);
        
        //Now load the Lists with data
        List<double[]> rowList = new ArrayList<>();
        List<Double> outputList = new ArrayList<>();

        MatrixValues mv = null;
        
        try (BufferedReader reader = Files.newBufferedReader(p, Charset.defaultCharset())) {
            String row;
            for (int i = 0; ; i++) {
                row = reader.readLine();
                if (row == null)
                    break;

                //Don't process the header row
                if (HEADER_ROW && i == 0)
                    continue;

                double[] rowArray = new double[NUM_FEATURES + 1];
                rowArray[0] = 1.0; //x0
                int column = 1; //Start at 1 due to x0
                
                //Split the row from the file
                String[] cells = row.split(DELIM);
                for (int j = 0; j < cells.length; j++) {

                    if (j == OUTPUT_COLUMN) {
                        outputList.add(Double.parseDouble(cells[j]));
                        continue;
                    }

                    //If there are more columns than features truncate the rest
                    if (column <= NUM_FEATURES)
                        rowArray[column++] = Double.parseDouble(cells[j]);
                }

                //Add row array to list
                rowList.add(rowArray);
            }

            //Now generate the Matrix and resultValues
            if (rowList.size() != outputList.size())
                throw new Exception("Row Mismatch in loadMatrixFromFile Method!");

            //Create the arrays
            double[][] matrix = new double[rowList.size()][NUM_FEATURES + 1]; //Add addtnl col for x0
            double[] resultValues = new double[outputList.size()];
        
            //Copy Lists into arrays
            for (int i = 0; i < rowList.size(); i++) {
                matrix[i] = (double[])rowList.get(i);
                resultValues[i] = (double)outputList.get(i);
            }
        
            mv = new MatrixValues(matrix, resultValues);
            
        } catch(Exception exc) {
            System.out.println(exc);
        }
        
        return mv;
    }
    
    
}
