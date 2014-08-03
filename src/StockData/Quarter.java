/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

/**
 *
 * @author Matt Jones
 */
public class Quarter {
    private int year;
    private int quarter;

    public Quarter(int year, int quarter) {
        this.year = year;
        this.quarter = quarter;
    }

    public int getYear() {
        return year;
    }

    public int getQuarter() {
        return quarter;
    }
}
