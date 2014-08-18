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
public class StockFundamentals_Quarter {
    private String ticker;
    private Date[] financials_Dates;
    private BigDecimal[] financials_Revenue;
    private BigDecimal[] financials_CostOfRev;
    private BigDecimal[] financials_GrossProfit;
    private BigDecimal[] financials_RandD;
    private BigDecimal[] financials_SalesGenAdmin;
    private BigDecimal[] financials_TotalOpExp;
    private BigDecimal[] financials_OperIncome;
    private BigDecimal[] financials_IntExp;
    private BigDecimal[] financials_OtherIncome;
    private BigDecimal[] financials_IncomeBeforeTax;
    private BigDecimal[] financials_ProvForIncTax;
    private BigDecimal[] financials_NetIncomeContOp;
    private BigDecimal[] financials_NetIncomeDiscontOp;
    private BigDecimal[] financials_NetIncome;
    private BigDecimal[] financials_NetIncomeCommonShareholders;
    private BigDecimal[] financials_EPS_Basic;
    private BigDecimal[] financials_EPS_Diluted;
    private BigDecimal[] financials_AvgSharesOutstanding_Basic;
    private BigDecimal[] financials_AvgSharesOutstanding_Diluted;
    private BigDecimal[] financials_EBITDA;

    public StockFundamentals_Quarter(String ticker) {
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

    public BigDecimal[] getFinancials_CostOfRev() {
        return financials_CostOfRev;
    }

    public void setFinancials_CostOfRev(BigDecimal[] financials_CostOfRev) {
        this.financials_CostOfRev = financials_CostOfRev;
    }

    public BigDecimal[] getFinancials_GrossProfit() {
        return financials_GrossProfit;
    }

    public void setFinancials_GrossProfit(BigDecimal[] financials_GrossProfit) {
        this.financials_GrossProfit = financials_GrossProfit;
    }

    public BigDecimal[] getFinancials_RandD() {
        return financials_RandD;
    }

    public void setFinancials_RandD(BigDecimal[] financials_RandD) {
        this.financials_RandD = financials_RandD;
    }

    public BigDecimal[] getFinancials_SalesGenAdmin() {
        return financials_SalesGenAdmin;
    }

    public void setFinancials_SalesGenAdmin(BigDecimal[] financials_SalesGenAdmin) {
        this.financials_SalesGenAdmin = financials_SalesGenAdmin;
    }

    public BigDecimal[] getFinancials_TotalOpExp() {
        return financials_TotalOpExp;
    }

    public void setFinancials_TotalOpExp(BigDecimal[] financials_TotalOpExp) {
        this.financials_TotalOpExp = financials_TotalOpExp;
    }

    public BigDecimal[] getFinancials_OperIncome() {
        return financials_OperIncome;
    }

    public void setFinancials_OperIncome(BigDecimal[] financials_OperIncome) {
        this.financials_OperIncome = financials_OperIncome;
    }

    public BigDecimal[] getFinancials_IncomeBeforeTax() {
        return financials_IncomeBeforeTax;
    }

    public void setFinancials_IncomeBeforeTax(BigDecimal[] financials_IncomeBeforeTax) {
        this.financials_IncomeBeforeTax = financials_IncomeBeforeTax;
    }

    public BigDecimal[] getFinancials_ProvForIncTax() {
        return financials_ProvForIncTax;
    }

    public void setFinancials_ProvForIncTax(BigDecimal[] financials_ProvForIncTax) {
        this.financials_ProvForIncTax = financials_ProvForIncTax;
    }

    public BigDecimal[] getFinancials_NetIncomeContOp() {
        return financials_NetIncomeContOp;
    }

    public void setFinancials_NetIncomeContOp(BigDecimal[] financials_NetIncomeContOp) {
        this.financials_NetIncomeContOp = financials_NetIncomeContOp;
    }

    public BigDecimal[] getFinancials_NetIncomeDiscontOp() {
        return financials_NetIncomeDiscontOp;
    }

    public void setFinancials_NetIncomeDiscontOp(BigDecimal[] financials_NetIncomeDiscontOp) {
        this.financials_NetIncomeDiscontOp = financials_NetIncomeDiscontOp;
    }

    public BigDecimal[] getFinancials_NetIncome() {
        return financials_NetIncome;
    }

    public void setFinancials_NetIncome(BigDecimal[] financials_NetIncome) {
        this.financials_NetIncome = financials_NetIncome;
    }

    public BigDecimal[] getFinancials_NetIncomeCommonShareholders() {
        return financials_NetIncomeCommonShareholders;
    }

    public void setFinancials_NetIncomeCommonShareholders(BigDecimal[] financials_NetIncomeCommonShareholders) {
        this.financials_NetIncomeCommonShareholders = financials_NetIncomeCommonShareholders;
    }

    public BigDecimal[] getFinancials_EPS_Basic() {
        return financials_EPS_Basic;
    }

    public void setFinancials_EPS_Basic(BigDecimal[] financials_EPS_Basic) {
        this.financials_EPS_Basic = financials_EPS_Basic;
    }

    public BigDecimal[] getFinancials_EPS_Diluted() {
        return financials_EPS_Diluted;
    }

    public void setFinancials_EPS_Diluted(BigDecimal[] financials_EPS_Diluted) {
        this.financials_EPS_Diluted = financials_EPS_Diluted;
    }

    public BigDecimal[] getFinancials_AvgSharesOutstanding_Basic() {
        return financials_AvgSharesOutstanding_Basic;
    }

    public void setFinancials_AvgSharesOutstanding_Basic(BigDecimal[] financials_AvgSharesOutstanding_Basic) {
        this.financials_AvgSharesOutstanding_Basic = financials_AvgSharesOutstanding_Basic;
    }

    public BigDecimal[] getFinancials_AvgSharesOutstanding_Diluted() {
        return financials_AvgSharesOutstanding_Diluted;
    }

    public void setFinancials_AvgSharesOutstanding_Diluted(BigDecimal[] financials_AvgSharesOutstanding_Diluted) {
        this.financials_AvgSharesOutstanding_Diluted = financials_AvgSharesOutstanding_Diluted;
    }

    public BigDecimal[] getFinancials_EBITDA() {
        return financials_EBITDA;
    }

    public void setFinancials_EBITDA(BigDecimal[] financials_EBITDA) {
        this.financials_EBITDA = financials_EBITDA;
    }

    public BigDecimal[] getFinancials_IntExp() {
        return financials_IntExp;
    }

    public void setFinancials_IntExp(BigDecimal[] financials_IntExp) {
        this.financials_IntExp = financials_IntExp;
    }

    public BigDecimal[] getFinancials_OtherIncome() {
        return financials_OtherIncome;
    }

    public void setFinancials_OtherIncome(BigDecimal[] financials_OtherIncome) {
        this.financials_OtherIncome = financials_OtherIncome;
    }

}
