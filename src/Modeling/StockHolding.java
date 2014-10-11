/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Modeling;

import java.util.Date;

/**
 *
 * @author Matt Jones
 */
public class StockHolding {
    private String ticker;
    private int sharesHeld;
    private Date targetDate;

    public StockHolding(String ticker, int sharesHeld, Date targetDate) {
        this.ticker = ticker;
        this.sharesHeld = sharesHeld;
        this.targetDate = targetDate;
    }

    public int getSharesHeld() {
        return sharesHeld;
    }

    public void setSharesHeld(int sharesHeld) {
        this.sharesHeld = sharesHeld;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public Date getTargetDate() {
        return targetDate;
    }

    public void setTargetDate(Date targetDate) {
        this.targetDate = targetDate;
    }
}
