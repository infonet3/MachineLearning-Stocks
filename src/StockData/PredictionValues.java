/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import java.math.BigDecimal;
import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class PredictionValues {
    private String ticker;
    private Date date;
    private Date projectedDate;
    private String modelType;
    private BigDecimal estimatedValue;
    private BigDecimal actualValue;

    public PredictionValues(String ticker, Date date, Date projectedDate, String modelType, BigDecimal estimatedValue) {
        this.ticker = ticker;
        this.date = date;
        this.projectedDate = projectedDate;
        this.modelType = modelType;
        this.estimatedValue = estimatedValue;
    }
    
    public PredictionValues(String ticker, Date date, Date projectedDate, String modelType, BigDecimal estimatedValue, BigDecimal actualValue) {
        this.ticker = ticker;
        this.date = date;
        this.projectedDate = projectedDate;
        this.modelType = modelType;
        this.estimatedValue = estimatedValue;
        this.actualValue = actualValue;
    }

    public String getTicker() {
        return ticker;
    }

    public Date getDate() {
        return date;
    }

    public Date getProjectedDate() {
        return projectedDate;
    }

    public String getModelType() {
        return modelType;
    }

    public BigDecimal getEstimatedValue() {
        return estimatedValue;
    }

    public BigDecimal getActualValue() {
        return actualValue;
    }

}
