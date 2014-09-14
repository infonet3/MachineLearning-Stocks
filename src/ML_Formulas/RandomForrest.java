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
            
            String newRow = String.valueOf(results[i]); //Set first item to the result
            
            for (int j = 0; j < cols; j++) {
                newRow += "," + String.valueOf(inputMatrix[i][j]);
            }

            strArray[i] = newRow;
        }
        
        return strArray;
    }
    
    private String buildDescriptor(int numCols) {
        String descriptor = "";
        for (int i = 0; i < numCols; i++) {
            if (i == 0)
                descriptor = "L";
            else
                descriptor += " N";
        }

        return descriptor;
    }
    
    public void createRandomForrest(final MatrixValues MATRIX_VALUES, final String TICKER) throws Exception {

        double[][] trainValues = MATRIX_VALUES.getFeatures(RecordType.TRAINING);
        double[] trainResults = MATRIX_VALUES.getOutputValues(RecordType.TRAINING);

        double[][] testValues = MATRIX_VALUES.getFeatures(RecordType.TEST);
        double[] testResults = MATRIX_VALUES.getOutputValues(RecordType.TEST);

        //Convert to Strings
        String[] trainStrValues = convertToStringArray(trainValues, trainResults);
        String[] testStrValues = convertToStringArray(testValues, testResults);
        
        //Create the descriptor - Put the target label at the end
        String descriptor = buildDescriptor(trainStrValues[0].split(",").length);

        //Now create the random forrest
        int numberOfTrees = 100;
        buildTree(numberOfTrees, trainStrValues, testStrValues, descriptor, TICKER, trainStrValues[0].split(",").length);
    }

    private void buildTree(int numberOfTrees, String[] trainDataValues, String[] testDataValues, String descriptor, String ticker, int cols) throws Exception {
        
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

        //Save the model to the file system
        String fileName = "C:\\Java\\RandomForrestModels\\" + ticker + ".txt";
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(fileName));
        forest.write(dos);
        
        //Load the model back from the file system
        Configuration config = new Configuration();
        Path path = new Path(fileName);
        DecisionForest f2 = DecisionForest.load(null, path);
        
        //Data test = DataLoader.loadData(data.getDataset(), testDataValues);
        Data test = DataLoader.loadData(data.getDataset(), testDataValues);
        int numberCorrect = 0;
        int numberOfValues = 0;

        for (int i = 0; i < test.size(); i++) {
            Instance oneSample = test.get(i);
            
            //Prediction
            double classify = forest.classify(test.getDataset(), rng, oneSample);
            String classifyStr = String.valueOf(classify);
            int label = data.getDataset().valueOf(0, classifyStr);
            
            //Actual
            double actualIndex = oneSample.get(0); 
            String actualStr = String.valueOf(actualIndex);
            int actualLabel = data.getDataset().valueOf(0, actualStr);

            //System.out.println("label = " + label + " actual = " + actualLabel);

            if (label == actualLabel) {
                numberCorrect++;
            }
            numberOfValues++;
        }

        double percentageCorrect = numberCorrect * 100.0 / numberOfValues;
        System.out.println("Number of trees: " + numberOfTrees + " -> Number correct: " + numberCorrect + " of " + numberOfValues + " (" + percentageCorrect + ")");
    }
}
