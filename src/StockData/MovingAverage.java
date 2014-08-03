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
public class MovingAverage {
    private String stockTicker;
    private Date date;
    private BigDecimal fiveDayMA;
    private BigDecimal twentyDayMA;
    private BigDecimal sixtyDayMA;

    public MovingAverage(String stockTicker, Date date, BigDecimal fiveDayMA, BigDecimal twentyDayMA, BigDecimal sixtyDayMA) {
        this.stockTicker = stockTicker;
        this.date = date;
        this.fiveDayMA = fiveDayMA;
        this.twentyDayMA = twentyDayMA;
        this.sixtyDayMA = sixtyDayMA;
    }

    public String getStockTicker() {
        return stockTicker;
    }

    public Date getDate() {
        return date;
    }

    public BigDecimal getFiveDayMA() {
        return fiveDayMA;
    }

    public BigDecimal getTwentyDayMA() {
        return twentyDayMA;
    }

    public BigDecimal getSixtyDayMA() {
        return sixtyDayMA;
    }
}
