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
public class BacktestingResults {
    private String ticker;
    private String modelType;
    private Date startDt;
    private Date endDt;
    private int numTrades;
    private BigDecimal assetValuePctChg;
    private BigDecimal buyAndHoldPctChg;

    public BacktestingResults(String ticker, String modelType, Date startDt, Date endDt, int numTrades, BigDecimal assetValuePctChg, BigDecimal buyAndHoldPctChg) {
        this.ticker = ticker;
        this.modelType = modelType;
        this.startDt = startDt;
        this.endDt = endDt;
        this.numTrades = numTrades;
        this.assetValuePctChg = assetValuePctChg;
        this.buyAndHoldPctChg = buyAndHoldPctChg;
    }

    public String getTicker() {
        return ticker;
    }

    public String getModelType() {
        return modelType;
    }

    public Date getStartDt() {
        return startDt;
    }

    public Date getEndDt() {
        return endDt;
    }

    public int getNumTrades() {
        return numTrades;
    }

    public BigDecimal getAssetValuePctChg() {
        return assetValuePctChg;
    }

    public BigDecimal getBuyAndHoldPctChg() {
        return buyAndHoldPctChg;
    }


}
