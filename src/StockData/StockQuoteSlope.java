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
public class StockQuoteSlope {
    private String ticker;
    private Date date;
    private int days;
    private BigDecimal slope;

    public StockQuoteSlope(String ticker, Date date, int days, BigDecimal slope) {
        this.ticker = ticker;
        this.date = date;
        this.days = days;
        this.slope = slope;
    }

    public String getTicker() {
        return ticker;
    }

    public Date getDate() {
        return date;
    }

    public int getDays() {
        return days;
    }

    public BigDecimal getSlope() {
        return slope;
    }
}
