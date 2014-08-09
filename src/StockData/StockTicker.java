/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

/**
 *
 * @author Matt Jones
 */
public class StockTicker {
    private String ticker;
    private String quandlCode;
    private String description;
    private String exchange;
    
    public StockTicker(String ticker, String quandlCode, String description, String exchange) {
        this.ticker = ticker;
        this.quandlCode = quandlCode;
        this.description = description;
        this.exchange = exchange;
    }

    public String getExchange() {
        return exchange;
    }
    
    public String getTicker() {
        return ticker;
    }

    public String getQuandlCode() {
        return quandlCode;
    }

    public String getDescription() {
        return description;
    }
    
    public String toString() {
        String str = "Ticker: " + ticker + ", Quandl_Code: " + quandlCode + ", Description: " + description;
        return str;
    }
}
