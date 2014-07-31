/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package MatrixOps;

import static MatrixOps.RecordType.CROSS_VALIDATION;
import static MatrixOps.RecordType.TEST;
import static MatrixOps.RecordType.TRAINING;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Matt Jones
 */
public class MatrixValues {
    //Fields
    private double[][] features;
    private double[] outputs;
    private RecordType[] recordTypes;

    //Take every feature, except x0, and raise to x^2, ..., x^degree
    public void featureMapping(int degree) throws Exception {
        if (degree <= 1)
            throw new Exception("Method: featureMapping, Description: degree must be greater than 1.");
        
        int numRows = features.length;
        int numFeatures = features[0].length - 1; //Remove x0
        int newNumFeatures = (numFeatures * degree) + 1; //Add x0 back in
        
        double[][] newFeatureMatrix = new double[numRows][newNumFeatures];

        double[] row;
        double[] polynomialTerms;
        for (int i = 0; i < numRows; i++) {
            row = features[i];
            polynomialTerms = polynomialValues(row, degree);
            System.arraycopy(polynomialTerms, 0, newFeatureMatrix[i], 0, newNumFeatures);
        }

        features = newFeatureMatrix;
    }
    
    private double[] polynomialValues(double[] originalValues, int degree) {
        List<Double> list = new ArrayList<>();
        
        for(int i = 0; i < originalValues.length; i++) {
            
            //Skip over x0 = 1
            if (i == 0) {
                list.add(1.0);
                continue;
            }
            
            //Generate polynomial terms
            double origValue = originalValues[i];
            for (int j = 0; j < degree; j++) {
                double newValue = Math.pow(origValue, j + 1);
                list.add(newValue);
            }
        }

        //Convert to array
        double[] newValues = new double[list.size()];
        for (int i = 0; i < newValues.length; i++) {
            newValues[i] = list.get(i);
        }
        
        return newValues;
    }
    
    public MatrixValues(double[][] featureValues, double[] outputValues) {
        
        this.features = featureValues;
        this.outputs = outputValues;
        this.recordTypes = new RecordType[outputValues.length];

        //Split up the training, cross validation, and test data sets
        double rand;
        for (int i = 0; i < recordTypes.length; i++) {
            rand = Math.random();
            
            //70% Training
            if (rand <= 0.7) {
                recordTypes[i] = RecordType.TRAINING;
            }
            //15% Cross Validation
            else if (rand > 0.7 && rand <= 0.85) {
                recordTypes[i] = RecordType.CROSS_VALIDATION;
            }
            //15% Test Data
            else {
                recordTypes[i] = RecordType.TEST;
            }
        }

        //Normalize the features
        meanNormalization();
    }
    
    public double[][] getFeatures(RecordType type) {
        
        List<double[]> featureList = new ArrayList<>();

        for (int i = 0; i < features.length; i++) {
            if (recordTypes[i] == type) {
                featureList.add(features[i]);
            }
        }

        //Convert back to array
        double[][] tmpFeatures = new double[featureList.size()][];
        
        for (int i = 0; i < tmpFeatures.length; i++) {
            tmpFeatures[i] = featureList.get(i);
        }
        
        return tmpFeatures;
    }

    public double[] getOutputValues(RecordType type) {
        
        List<Double> outputList = new ArrayList<>();

        for (int i = 0; i < outputs.length; i++) {
            if (recordTypes[i] == type) {
                outputList.add(outputs[i]);
            }
        }

        //Convert back to array
        double[] tmpOutputs = new double[outputList.size()];
        
        for (int i = 0; i < tmpOutputs.length; i++) {
            tmpOutputs[i] = outputList.get(i);
        }
        
        return tmpOutputs;
    }

    public double[] getAverages() {
        int rows = features.length;
        int cols = features[0].length;
        double[] averages = new double[cols];
        
        //Initialize the array
        for (int i = 0; i < cols; i++) {
            averages[i] = 0.0;
        }

        //Sum the columns
        for (int i = 0; i < cols; i++) {
            for (int j = 0; j < rows; j++) {
                averages[i] += features[j][i];
            }
        }
        
        //Find the average
        for (int i = 0; i < averages.length; i++) {
            averages[i] /= rows; 
        }
        
        return averages;
    }

    public double[] getRanges() {
        int rows = features.length;
        int cols = features[0].length;
        double[] ranges = new double[cols];
        
        //Initialize the array
        for (int i = 0; i < cols; i++) {
            ranges[i] = 0.0;
        }

        //Sum the columns
        double max = 0.0;
        double min = 0.0;
        for (int i = 0; i < cols; i++) {
            for (int j = 0; j < rows; j++) {
                
                //Initialize max and min to the first element
                if (j == 0) {
                    max = min = features[j][i];
                } else {
                    if (features[j][i] < min) {
                        min = features[j][i];
                    }
                    
                    if (features[j][i] > max) {
                        max = features[j][i];
                    }
                }
            }
            
            ranges[i] = max - min;
        }

        return ranges;
    }

    private void meanNormalization() {

        double[] averages = getAverages();
        double[] ranges = getRanges();
        
        int rows = features.length;
        int cols = features[0].length;
        double[][] normalizedMatrix = new double[rows][cols];

        //Normalize the matrix
        for (int i = 0; i < cols; i++) {
            for (int j = 0; j < rows; j++) {
                if (i == 0) {
                    normalizedMatrix[j][i] = features[j][i]; //Skip x0
                } else {
                    //Protect against a feature that is constant for all training examples
                    if (ranges[i] == 0.0)
                        normalizedMatrix[j][i] = features[j][i] / averages[i]; //Will set constant feature to 1
                    else
                        normalizedMatrix[j][i] = (features[j][i] - averages[i]) / ranges[i];
                }
            }
        }
        
        features = normalizedMatrix;
    }

}
