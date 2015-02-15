/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import java.math.BigDecimal;
import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class FuturePrice implements Comparable {
    private String ticker;
    private double forecastPctChg;
    private BigDecimal currentPrice;
    private BigDecimal predictedPrice;
    private Date projectedDt;

    public FuturePrice(String ticker, double forecastPctChg, BigDecimal currentPrice, BigDecimal predictedPrice, Date projectedDt) {
        this.ticker = ticker;
        this.forecastPctChg = forecastPctChg;
        this.currentPrice = currentPrice;
        this.predictedPrice = predictedPrice;
        this.projectedDt = projectedDt;
    }

    @Override
    public int compareTo(Object obj) {
        
        FuturePrice fp2 = (FuturePrice)obj;
        
        if (forecastPctChg < fp2.forecastPctChg)
            return -1;
        else if (forecastPctChg == fp2.forecastPctChg)
            return 0;
        else
            return 1;
    }
    
    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public double getForecastPctChg() {
        return forecastPctChg;
    }

    public void setForecastPctChg(double forecastPctChg) {
        this.forecastPctChg = forecastPctChg;
    }

    public BigDecimal getCurrentPrice() {
        return currentPrice;
    }

    public Date getProjectedDt() {
        return projectedDt;
    }

    public void setProjectedDt(Date projectedDt) {
        this.projectedDt = projectedDt;
    }

    public BigDecimal getPredictedPrice() {
        return predictedPrice;
    }

    public void setPredictedPrice(BigDecimal predictedPrice) {
        this.predictedPrice = predictedPrice;
    }
}
