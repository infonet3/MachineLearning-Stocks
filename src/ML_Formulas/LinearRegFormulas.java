/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ML_Formulas;

/**
 *
 * @author Matt Jones
 */
public class LinearRegFormulas {
    
    private static final double ALPHA = 0.03;
    
    public static double costFunction(double[][] inputMatrix, double[] theta, double[] result, double lambda) {
        double sum = 0.0;
        double variance = 0.0;
        for (int i = 0; i < inputMatrix.length; i++) {
            variance = hypothesis(inputMatrix[i], theta) - result[i];
            sum += Math.pow(variance, 2);
        }
        
        //Add in regularization if needed, skip x0
        double regularization = 0.0;
        for (int i = 1; i < theta.length; i++) {
            regularization += Math.pow(theta[i], 2);
        }
        
        double m = inputMatrix.length;
        return (1.0 / (2.0 * m)) * (sum + (lambda * regularization));
    }
    
    public static double hypothesis(double[] inputs, double[] theta) {
        
        double sum = 0.0;
        
        for (int i = 0; i < inputs.length; i++) {
            sum += inputs[i] * theta[i];
        }
        
        return sum;
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
            double m = inputMatrix.length;
            if (i == 0)
                newTheta[i] = theta[i] - ALPHA * (1.0 / m) * sum;
            else
                newTheta[i] = theta[i] - ALPHA * ((1.0 / m) * sum + ((lambda / m) * theta[i]));
        }
  
        //Update theta
        System.arraycopy(newTheta, 0, theta, 0, theta.length);
    }
}
