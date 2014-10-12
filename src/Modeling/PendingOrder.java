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
public class PendingOrder {

    private OrderType type;
    private String ticker;
    private int numShares;
    private Date projectedDt;

    public PendingOrder(OrderType type, String ticker, int numShares, Date projectedDt) {
        this.type = type;
        this.ticker = ticker;
        this.numShares = numShares;
        this.projectedDt = projectedDt;
    }

    public int getNumShares() {
        return numShares;
    }

    public void setNumShares(int numShares) {
        this.numShares = numShares;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public Date getProjectedDt() {
        return projectedDt;
    }

    public void setProjectedDt(Date projectedDt) {
        this.projectedDt = projectedDt;
    }

    public OrderType getType() {
        return type;
    }

    public void setType(OrderType type) {
        this.type = type;
    }
}
