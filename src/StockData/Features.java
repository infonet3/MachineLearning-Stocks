/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class Features {
    private Date date;
    private double[] featureValues;

    public Features(Date date, double[] featureValues) {
        this.date = date;
        this.featureValues = featureValues;
    }

    public Date getDate() {
        return date;
    }

    public double[] getFeatureValues() {
        return featureValues;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setFeatureValues(double[] featureValues) {
        this.featureValues = featureValues;
    }
}
