/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import java.math.BigDecimal;

/**
 *
 * @author Matt Jones
 */
public class Weight {
    private int id;
    private BigDecimal theta;
    private BigDecimal average;
    private BigDecimal range;

    public Weight(int id, BigDecimal theta, BigDecimal average, BigDecimal range) {
        this.id = id;
        this.theta = theta;
        this.average = average;
        this.range = range;
    }

    public int getId() {
        return id;
    }

    public BigDecimal getTheta() {
        return theta;
    }

    public BigDecimal getAverage() {
        return average;
    }

    public BigDecimal getRange() {
        return range;
    }
}
