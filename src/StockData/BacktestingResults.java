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
    private Date startDt;
    private Date endDt;
    private BigDecimal assetValue;
    private BigDecimal pctChg;
    private BigDecimal sp500PctChg;

    public BacktestingResults(Date startDt, Date endDt, BigDecimal assetValue, BigDecimal pctChg, BigDecimal sp500PctChg) {
        this.startDt = startDt;
        this.endDt = endDt;
        this.assetValue = assetValue;
        this.pctChg = pctChg;
        this.sp500PctChg = sp500PctChg;
    }

    public Date getStartDt() {
        return startDt;
    }

    public void setStartDt(Date startDt) {
        this.startDt = startDt;
    }

    public Date getEndDt() {
        return endDt;
    }

    public void setEndDt(Date endDt) {
        this.endDt = endDt;
    }

    public BigDecimal getAssetValue() {
        return assetValue;
    }

    public void setAssetValue(BigDecimal assetValue) {
        this.assetValue = assetValue;
    }

    public BigDecimal getPctChg() {
        return pctChg;
    }

    public void setPctChg(BigDecimal pctChg) {
        this.pctChg = pctChg;
    }

    public BigDecimal getSp500PctChg() {
        return sp500PctChg;
    }

    public void setSp500PctChg(BigDecimal sp500PctChg) {
        this.sp500PctChg = sp500PctChg;
    }

}
