/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import java.math.BigDecimal;

/**
 *
 * @author Matt Jones
 */
public class Portfolio {
    private StockHolding[] stockList;
    private BigDecimal cash;

    public Portfolio(final int MAX_STOCKS) {
        this.stockList = new StockHolding[MAX_STOCKS];
        cash = new BigDecimal("0.0");
    }
}
