/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ML_Formulas;

import MatrixOps.MatrixValues;
import MatrixOps.RecordType;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.builder.DefaultTreeBuilder;
import org.apache.mahout.classifier.df.data.Data;
import org.apache.mahout.classifier.df.data.DataLoader;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.data.Instance;
import org.apache.mahout.classifier.df.ref.SequentialBuilder;
import org.apache.mahout.common.RandomUtils;

/**
 *
 * @author Matt Jones
 */
public class RandomForrest {
    
    private String[] convertToStringArray(double[][] inputMatrix, double[] results) {
        
        int rows = inputMatrix.length;
        int cols = inputMatrix[0].length;
        
        String[] strArray = new String[rows];
        for (int i = 0; i < rows; i++) {
            
            StringBuilder newRow = new StringBuilder();
            newRow.append(results[i]); //Set first item to the result
            
            for (int j = 0; j < cols; j++) {
                newRow.append(",");
                newRow.append(String.valueOf(inputMatrix[i][j]));
            }

            strArray[i] = newRow.toString();
        }
        
        return strArray;
    }
    
    private String buildDescriptor(int numCols) {
        StringBuilder descriptor = new StringBuilder();
        for (int i = 0; i < numCols; i++) {
            if (i == 0)
                descriptor.append("L");
            else
                descriptor.append(" N");
        }

        return descriptor.toString();
    }
    
    public DecisionForest createRandomForrest(final MatrixValues MATRIX_VALUES, int numTrees) throws Exception {

        //Create the String values
        double[][] trainValues = MATRIX_VALUES.getFeatures(RecordType.TRAINING);
        double[] trainResults = MATRIX_VALUES.getOutputValues(RecordType.TRAINING);
        String[] trainStrValues = convertToStringArray(trainValues, trainResults);
        
        //Create the descriptor - Put the target label at the end
        int cols = trainStrValues[0].split(",").length;
        String descriptor = buildDescriptor(cols);

        //Now create the random forrest
        return buildTree(numTrees, trainStrValues, descriptor, cols);
    }

    private DecisionForest buildTree(int numberOfTrees, String[] trainDataValues, String descriptor, int cols) throws Exception {
        
        //Load the training data
        Dataset dataset = DataLoader.generateDataset(descriptor, false, trainDataValues); //2nd parameter is for regression
        Data data = DataLoader.loadData(dataset, trainDataValues);
        
        //Make this log base 2
        double num = Math.log10(data.getDataset().nbAttributes());
        double denom = Math.log10(2);
        int m = (int) (num / denom) + 1;
        
        //Prepare the tree
        Random rng = RandomUtils.getRandom();
        DefaultTreeBuilder treeBuilder = new DefaultTreeBuilder();
        SequentialBuilder forestBuilder = new SequentialBuilder(RandomUtils.getRandom(), treeBuilder, data.clone());
        treeBuilder.setM(m);  //number of random variables to select at each tree-node
        DecisionForest forest = forestBuilder.build(numberOfTrees);

        return forest;
    }
    
    public void saveForestToFile(DecisionForest forest, String ticker) throws Exception {

        String fileName = "C:\\Java\\RandomForrestModels\\" + ticker + ".txt";
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(fileName));
        forest.write(dos);
    }
    
    public DecisionForest loadForestFromFile(String ticker) throws Exception {

        Configuration config = new Configuration();

        String fileName = "C:\\Java\\RandomForrestModels\\" + ticker + ".txt";
        Path path = new Path(fileName);
        
        DecisionForest df = DecisionForest.load(config, path);
        return df;
    }
    
    public double hypothesis(DecisionForest df, double[] row) throws Exception {

        //Create the String values
        double[][] inputMatrix = new double[1][];
        inputMatrix[0] = row;
        
        double[] result = new double[1];
        
        String[] strArray = convertToStringArray(inputMatrix, result);
        
        //Create the descriptor
        int cols = strArray[0].split(",").length;
        String descriptor = buildDescriptor(cols);

        //Load the dataset
        Dataset dataset = DataLoader.generateDataset(descriptor, false, strArray); //2nd parameter is for regression
        Data test = DataLoader.loadData(dataset, strArray);
            
        int numRows = test.size();
        double[][] preds = new double[numRows][];
        df.classify(test, preds);

        //Loop through tree predictions
        int posCount = 0;
        int numTrees = preds[0].length;
        for (int i = 0; i < numTrees; i++) {
            if (preds[0][i] == 1.0)
                posCount++;
        }

        //If half or more trees say positive then vote positive
        boolean isPositive = false;
        if (posCount >= (numTrees / 2.0))
            isPositive = true;

        //Return value
        if (isPositive)
            return 1.0;
        else
            return 0.0;
    }
    
    public CostResults testRandomForrest(DecisionForest df, double[][] features, double[] outputs, int numTrees) throws Exception {

        double percentageCorrect = 0;
        try {
            //Create the String values
            String[] testDataValues = convertToStringArray(features, outputs);

            //Create the descriptor - Put the target label at the end
            int cols = testDataValues[0].split(",").length;
            String descriptor = buildDescriptor(cols);

            //Load the dataset
            Dataset dataset = DataLoader.generateDataset(descriptor, false, testDataValues); //2nd parameter is for regression
            Data test = DataLoader.loadData(dataset, testDataValues);

            int numberCorrect = 0;
            int numberOfValues = test.size();
            
            int numRows = test.size();
            double[][] preds = new double[numRows][];
            df.classify(test, preds);
            double[] row;

            //Loop through the rows
            for (int i = 0; i < numRows; i++) {
                row = preds[i];
                int posCount = 0;
                
                //Loop through the tree predictions
                for (int j = 0; j < numTrees; j++) {
                    if (row[j] == 1.0)
                        posCount++;
                }
                
                //If half or more trees say positive then vote positive
                boolean isPositive = false;
                if (posCount >= (numTrees / 2.0))
                    isPositive = true;

                //See if the prediction matches the actual values
                if (isPositive && outputs[i] == 1.0 || !isPositive && outputs[i] == 0.0)
                    numberCorrect++;
            }
            
            percentageCorrect = numberCorrect * 100.0 / numberOfValues;
            
            /*
            Random rng = RandomUtils.getRandom();
            for (int i = 0; i < test.size(); i++) {
                
                Instance oneSample = test.get(i);
                
                //Prediction
                int label = -1;
                try {
                    double classify = df.classify(test.getDataset(), rng, oneSample);
                    String classifyStr = Double.toString(classify);
                    label = test.getDataset().valueOf(0, classifyStr);
                } catch (Exception exc) {
                    System.out.println("Classify Problem: " + exc);
                }

                //Actual
                double actualValue = oneSample.get(0); 
                String actualStr = String.valueOf(actualValue);
                int actualLabel = test.getDataset().valueOf(0, actualStr);
                
                //System.out.println("label = " + label + " actual = " + actualLabel);

                if (label == actualLabel) {
                    numberCorrect++;
                }

                numberOfValues++;
            }

            percentageCorrect = numberCorrect * 100.0 / numberOfValues;
          */
          
        } catch(Exception exc) {
            System.out.println("Method: testRandomForest, Desc: " + exc);
            throw exc;
        }
        
        CostResults results = new CostResults(0, percentageCorrect);
        return results;
    }
}
