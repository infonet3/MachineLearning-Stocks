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
    private String predType;
    private String modelType;
    private BigDecimal estimatedValue;
    private BigDecimal curCloseValue;
    private BigDecimal futureValue;
    private BigDecimal curOpenValue;

    //pred.pk_DateID, pred.pk_ProjectedDateID, pred.EstimatedValue, curQuotes.Close, futureQuotes.Close, curQuotes.Open
    public PredictionValues(String ticker, Date date, Date projectedDate, String modelType, BigDecimal estimatedValue, BigDecimal curCloseValue, BigDecimal futureValue, BigDecimal curOpenValue) {
        this.ticker = ticker;
        this.date = date;
        this.projectedDate = projectedDate;
        this.modelType = modelType;
        this.estimatedValue = estimatedValue;
        this.curCloseValue = curCloseValue;
        this.futureValue = futureValue;
        this.curOpenValue = curOpenValue;
    }

    //ticker.getTicker(), curDates[i].getTime(), targetDates[i].getTime(), MODEL_TYPE.toString(), bd
    public PredictionValues(String ticker, Date date, Date projectedDate, String modelType, String predType, BigDecimal estimatedValue) {
        this.ticker = ticker;
        this.date = date;
        this.projectedDate = projectedDate;
        this.modelType = modelType;
        this.predType = predType;
        this.estimatedValue = estimatedValue;
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

    public String getPredType() {
        return predType;
    }
    
}
