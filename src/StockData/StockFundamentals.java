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
public class StockFundamentals {
    private String ticker;
    private Date[] financials_Dates;
    private BigDecimal[] financials_Revenue;
    private BigDecimal[] financials_GrossMargin;
    private BigDecimal[] financials_OperIncome;
    private BigDecimal[] financials_OperMargin;
    private BigDecimal[] financials_NetIncome;
    private BigDecimal[] financials_EPS;
    private BigDecimal[] financials_Dividends;
    private BigDecimal[] financials_PayoutRatio;
    private BigDecimal[] financials_SharesMil;
    private BigDecimal[] financials_BookValPerShare;
    private BigDecimal[] financials_OperCashFlow;
    private BigDecimal[] financials_CapSpending;
    private BigDecimal[] financials_FreeCashFlow;
    private BigDecimal[] financials_FreeCashFlowPerShare;
    private BigDecimal[] financials_WorkingCap;
    private BigDecimal[] financials_ReturnOnAssets;
    private BigDecimal[] financials_ReturnOnEquity;

    public StockFundamentals(String ticker) {
        this.ticker = ticker;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public Date[] getFinancials_Dates() {
        return financials_Dates;
    }

    public void setFinancials_Dates(Date[] financials_Dates) {
        this.financials_Dates = financials_Dates;
    }

    public BigDecimal[] getFinancials_Revenue() {
        return financials_Revenue;
    }

    public void setFinancials_Revenue(BigDecimal[] financials_Revenue) {
        this.financials_Revenue = financials_Revenue;
    }

    public BigDecimal[] getFinancials_GrossMargin() {
        return financials_GrossMargin;
    }

    public void setFinancials_GrossMargin(BigDecimal[] financials_GrossMargin) {
        this.financials_GrossMargin = financials_GrossMargin;
    }

    public BigDecimal[] getFinancials_OperIncome() {
        return financials_OperIncome;
    }

    public void setFinancials_OperIncome(BigDecimal[] financials_OperIncome) {
        this.financials_OperIncome = financials_OperIncome;
    }

    public BigDecimal[] getFinancials_OperMargin() {
        return financials_OperMargin;
    }

    public void setFinancials_OperMargin(BigDecimal[] financials_OperMargin) {
        this.financials_OperMargin = financials_OperMargin;
    }

    public BigDecimal[] getFinancials_NetIncome() {
        return financials_NetIncome;
    }

    public void setFinancials_NetIncome(BigDecimal[] financials_NetIncomeBD) {
        this.financials_NetIncome = financials_NetIncomeBD;
    }

    public BigDecimal[] getFinancials_EPS() {
        return financials_EPS;
    }

    public void setFinancials_EPS(BigDecimal[] financials_EPS) {
        this.financials_EPS = financials_EPS;
    }

    public BigDecimal[] getFinancials_Dividends() {
        return financials_Dividends;
    }

    public void setFinancials_Dividends(BigDecimal[] financials_Dividends) {
        this.financials_Dividends = financials_Dividends;
    }

    public BigDecimal[] getFinancials_PayoutRatio() {
        return financials_PayoutRatio;
    }

    public void setFinancials_PayoutRatio(BigDecimal[] financials_PayoutRatio) {
        this.financials_PayoutRatio = financials_PayoutRatio;
    }

    public BigDecimal[] getFinancials_SharesMil() {
        return financials_SharesMil;
    }

    public void setFinancials_SharesMil(BigDecimal[] financials_SharesMil) {
        this.financials_SharesMil = financials_SharesMil;
    }

    public BigDecimal[] getFinancials_BookValPerShare() {
        return financials_BookValPerShare;
    }

    public void setFinancials_BookValPerShare(BigDecimal[] financials_BookValPerShare) {
        this.financials_BookValPerShare = financials_BookValPerShare;
    }

    public BigDecimal[] getFinancials_OperCashFlow() {
        return financials_OperCashFlow;
    }

    public void setFinancials_OperCashFlow(BigDecimal[] financials_OperCashFlow) {
        this.financials_OperCashFlow = financials_OperCashFlow;
    }

    public BigDecimal[] getFinancials_CapSpending() {
        return financials_CapSpending;
    }

    public void setFinancials_CapSpending(BigDecimal[] financials_CapSpending) {
        this.financials_CapSpending = financials_CapSpending;
    }

    public BigDecimal[] getFinancials_FreeCashFlow() {
        return financials_FreeCashFlow;
    }

    public void setFinancials_FreeCashFlow(BigDecimal[] financials_FreeCashFlow) {
        this.financials_FreeCashFlow = financials_FreeCashFlow;
    }

    public BigDecimal[] getFinancials_FreeCashFlowPerShare() {
        return financials_FreeCashFlowPerShare;
    }

    public void setFinancials_FreeCashFlowPerShare(BigDecimal[] financials_FreeCashFlowPerShare) {
        this.financials_FreeCashFlowPerShare = financials_FreeCashFlowPerShare;
    }

    public BigDecimal[] getFinancials_WorkingCap() {
        return financials_WorkingCap;
    }

    public void setFinancials_WorkingCap(BigDecimal[] financials_WorkingCap) {
        this.financials_WorkingCap = financials_WorkingCap;
    }

    public BigDecimal[] getFinancials_ReturnOnAssets() {
        return financials_ReturnOnAssets;
    }

    public void setFinancials_ReturnOnAssets(BigDecimal[] financials_ReturnOnAssets) {
        this.financials_ReturnOnAssets = financials_ReturnOnAssets;
    }

    public BigDecimal[] getFinancials_ReturnOnEquity() {
        return financials_ReturnOnEquity;
    }

    public void setFinancials_ReturnOnEquity(BigDecimal[] financials_ReturnOnEquity) {
        this.financials_ReturnOnEquity = financials_ReturnOnEquity;
    }

}
