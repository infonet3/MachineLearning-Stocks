/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ML_Formulas;

/**
 *
 * @author Matt Jones
 */
public class LogisticRegFormulas {

    private static final double ALPHA = 0.006; //Use for linear hypothesis
    
    public static CostResults costFunction(double[][] inputMatrix, double[] theta, double[] result, double lambda) {
        
        int numExamples = inputMatrix.length;
        int numCorrectPredictions = 0;
        double sum = 0.0;
        for (int i = 0; i < numExamples; i++) {

            double curHypothesis = hypothesis(inputMatrix[i], theta);

            //Sum the cost of errors
            sum += (result[i] * Math.log(curHypothesis)) + ((1 - result[i]) * Math.log(1 - curHypothesis));
            
            //Add up the number of correct predictions
            if ((curHypothesis >= 0.50 && result[i] == 1) || (curHypothesis < 0.50 && result[i] == 0)) {
                numCorrectPredictions++;
            }
        }
        
        //Add in regularization if needed, skip x0
        double regularization = 0.0;
        for (int i = 1; i < theta.length; i++) {
            regularization += Math.pow(theta[i], 2);
        }
        
        double cost = -((1.0 / inputMatrix.length) * sum) + ((lambda / (2 * inputMatrix.length)) * regularization);
        double accuracy = (double)numCorrectPredictions / (double)numExamples;
        
        CostResults results = new CostResults(cost, accuracy);
        return results;
    }

    //Sigmoid Function = 1 / (1 + e^-z)
    public static double hypothesis(double[] inputs, double[] theta) {
        
        double sum = 0.0;
        
        for (int i = 0; i < inputs.length; i++) {
            sum += inputs[i] * theta[i];
        }
        
        return 1.0 / (1.0 + Math.pow(Math.E, -sum));
    }
    
    public static void gradientDescent(double[][] inputMatrix, double[] theta, double[] result, double lambda) {
        double[] newTheta = new double[theta.length];
        
        double sum = 0.0;
        double val = 0.0;
        for (int i = 0; i < theta.length; i++) {

            //Summation
            for (int j = 0; j < inputMatrix.length; j++) {
                val = hypothesis(inputMatrix[j], theta) - result[j];
                val *= inputMatrix[j][i]; 
                
                sum += val;
            }

            //Regularization
            if (i == 0)
                newTheta[i] = theta[i] - ALPHA * (1.0 / inputMatrix.length) * sum;
            else
                newTheta[i] = theta[i] - ALPHA * ((1.0 / inputMatrix.length) * sum + ((lambda / inputMatrix.length) * theta[i]));
        }

        //Update values of theta
        System.arraycopy(newTheta, 0, theta, 0, theta.length);
    }
    
}
