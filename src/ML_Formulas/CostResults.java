/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ML_Formulas;

/**
 *
 * @author Matt Jones
 */
public class CostResults {
    private double cost;
    private double accuracy;

    public CostResults(double cost, double accuracy) {
        this.cost = cost;
        this.accuracy = accuracy;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(double accuracy) {
        this.accuracy = accuracy;
    }


}
