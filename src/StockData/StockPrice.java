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
    
    //Methods
    public StockPrice(Date date, BigDecimal price, BigDecimal volume) {
        this.date = date;
        this.price = price;
        this.volume = volume;
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
}
