/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import Modeling.PredictionType;
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
    private PredictionType predType;
    private String modelType;
    private BigDecimal estimatedValue;
    private BigDecimal curCloseValue;
    private BigDecimal futureValue;
    private BigDecimal curOpenValue;

    public PredictionValues(String ticker, Date date, Date projectedDate, String modelType, PredictionType predType, BigDecimal estimatedValue) {
        this.ticker = ticker;
        this.date = date;
        this.projectedDate = projectedDate;
        this.modelType = modelType;
        this.predType = predType;
        this.estimatedValue = estimatedValue;
    }

    public PredictionValues(String ticker, Date date, Date projectedDate, String modelType, BigDecimal estimatedValue, BigDecimal curCloseValue,
            BigDecimal futureValue, BigDecimal curOpenValue) {
        
        this.ticker = ticker;
        this.date = date;
        this.projectedDate = projectedDate;
        this.modelType = modelType;
        this.estimatedValue = estimatedValue;
        this.curCloseValue = curCloseValue;
        this.futureValue = futureValue;
        this.curOpenValue = curOpenValue;
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

    public BigDecimal getCurCloseValue() {
        return curCloseValue;
    }

    public BigDecimal getFutureValue() {
        return futureValue;
    }

    public BigDecimal getCurOpenValue() {
        return curOpenValue;
    }

    public PredictionType getPredType() {
        return predType;
    }
    
}
