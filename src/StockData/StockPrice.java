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
public class StockPrice {
    
    //Fields
    private Date date;
    private BigDecimal price;
    private BigDecimal volume;

    private BigDecimal fiveDayMA;
    private BigDecimal twentyDayMA;
    private BigDecimal sixtyDayMA;

    private BigDecimal fiveDayVolMA;
    private BigDecimal twentyDayVolMA;
    private BigDecimal sixtyDayVolMA;
    
    private BigDecimal fiveDaySlope;
    private BigDecimal twentyDaySlope;
    private BigDecimal sixtyDaySlope;

    //Methods
    public StockPrice(Date date, BigDecimal fiveDayMA, BigDecimal fiveDaySlope, BigDecimal twentyDaySlope, BigDecimal sixtyDaySlope) {
        this.date = date;
        this.fiveDayMA = fiveDayMA;
        this.fiveDaySlope = fiveDaySlope;
        this.twentyDaySlope = twentyDaySlope;
        this.sixtyDaySlope = sixtyDaySlope;
    }
    
    public StockPrice(Date date, BigDecimal price, BigDecimal volume, BigDecimal fiveDayMA, BigDecimal twentyDayMA, BigDecimal sixtyDayMA, BigDecimal fiveDayVolMA, BigDecimal twentyDayVolMA, BigDecimal sixtyDayVolMA) {
        this.date = date;
        this.price = price;
        this.volume = volume;
        this.fiveDayMA = fiveDayMA;
        this.twentyDayMA = twentyDayMA;
        this.sixtyDayMA = sixtyDayMA;
        this.fiveDayVolMA = fiveDayVolMA;
        this.twentyDayVolMA = twentyDayVolMA;
        this.sixtyDayVolMA = sixtyDayVolMA;
    }

    public StockPrice(Date date, BigDecimal price) {
        this.date = date;
        this.price = price;
    }

    public Date getDate() {
        return date;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public BigDecimal getVolume() {
        return volume;
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

    public BigDecimal getFiveDayVolMA() {
        return fiveDayVolMA;
    }

    public BigDecimal getTwentyDayVolMA() {
        return twentyDayVolMA;
    }

    public BigDecimal getSixtyDayVolMA() {
        return sixtyDayVolMA;
    }

    public BigDecimal getFiveDaySlope() {
        return fiveDaySlope;
    }

    public BigDecimal getTwentyDaySlope() {
        return twentyDaySlope;
    }

    public BigDecimal getSixtyDaySlope() {
        return sixtyDaySlope;
    }

}
