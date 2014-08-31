/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package StockData;

import java.math.BigDecimal;

/**
 *
 * @author Matt Jones
 */
public class BEA_Data {
    //Fields
    private int year;
    private int quarter;
    private BigDecimal grossPrivDomInv;
    private BigDecimal fixInvestment;
    private BigDecimal nonResidential;
    private BigDecimal residential;
    private BigDecimal chgPrivInventories;
    private BigDecimal netExportsGoodsAndSvc;
    private BigDecimal GDP;
    private BigDecimal goods1;
    private BigDecimal goods2;
    private BigDecimal goods3;
    private BigDecimal services1;
    private BigDecimal services2;
    private BigDecimal services3;
    private BigDecimal govConsExpAndGrossInv;
    private BigDecimal federal;
    private BigDecimal natDefense;
    private BigDecimal nonDefense;
    private BigDecimal stateAndLocal;
    private BigDecimal structures;
    private BigDecimal exports;
    private BigDecimal imports;
    private BigDecimal durableGoods;
    private BigDecimal nonDurGoods;
    private BigDecimal persConsExp;
    private BigDecimal intPropProducts;
    private BigDecimal equipment;
    
    //Methods
    public BEA_Data(int year, int quarter) {
        this.year = year;
        this.quarter = quarter;
    }

    public int getQuarter() {
        return quarter;
    }

    public void setQuarter(int quarter) {
        this.quarter = quarter;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public BigDecimal getGrossPrivDomInv() {
        return grossPrivDomInv;
    }

    public void setGrossPrivDomInv(BigDecimal grossPrivDomInv) {
        this.grossPrivDomInv = grossPrivDomInv;
    }

    public BigDecimal getFixInvestment() {
        return fixInvestment;
    }

    public void setFixInvestment(BigDecimal fixInvestment) {
        this.fixInvestment = fixInvestment;
    }

    public BigDecimal getNonResidential() {
        return nonResidential;
    }

    public void setNonResidential(BigDecimal nonResidential) {
        this.nonResidential = nonResidential;
    }

    public BigDecimal getResidential() {
        return residential;
    }

    public void setResidential(BigDecimal residential) {
        this.residential = residential;
    }

    public BigDecimal getChgPrivInventories() {
        return chgPrivInventories;
    }

    public void setChgPrivInventories(BigDecimal chgPrivInventories) {
        this.chgPrivInventories = chgPrivInventories;
    }

    public BigDecimal getNetExportsGoodsAndSvc() {
        return netExportsGoodsAndSvc;
    }

    public void setNetExportsGoodsAndSvc(BigDecimal netExportsGoodsAndSvc) {
        this.netExportsGoodsAndSvc = netExportsGoodsAndSvc;
    }

    public BigDecimal getGDP() {
        return GDP;
    }

    public void setGDP(BigDecimal GDP) {
        this.GDP = GDP;
    }

    public BigDecimal getGoods1() {
        return goods1;
    }

    public void setGoods1(BigDecimal goods1) {
        this.goods1 = goods1;
    }

    public BigDecimal getGoods2() {
        return goods2;
    }

    public void setGoods2(BigDecimal goods2) {
        this.goods2 = goods2;
    }

    public BigDecimal getGoods3() {
        return goods3;
    }

    public void setGoods3(BigDecimal goods3) {
        this.goods3 = goods3;
    }

    public BigDecimal getServices1() {
        return services1;
    }

    public void setServices1(BigDecimal services1) {
        this.services1 = services1;
    }

    public BigDecimal getServices2() {
        return services2;
    }

    public void setServices2(BigDecimal services2) {
        this.services2 = services2;
    }

    public BigDecimal getServices3() {
        return services3;
    }

    public void setServices3(BigDecimal services3) {
        this.services3 = services3;
    }

    public BigDecimal getGovConsExpAndGrossInv() {
        return govConsExpAndGrossInv;
    }

    public void setGovConsExpAndGrossInv(BigDecimal govConsExpAndGrossInv) {
        this.govConsExpAndGrossInv = govConsExpAndGrossInv;
    }

    public BigDecimal getFederal() {
        return federal;
    }

    public void setFederal(BigDecimal federal) {
        this.federal = federal;
    }

    public BigDecimal getNatDefense() {
        return natDefense;
    }

    public void setNatDefense(BigDecimal natDefense) {
        this.natDefense = natDefense;
    }

    public BigDecimal getNonDefense() {
        return nonDefense;
    }

    public void setNonDefense(BigDecimal nonDefense) {
        this.nonDefense = nonDefense;
    }

    public BigDecimal getStateAndLocal() {
        return stateAndLocal;
    }

    public void setStateAndLocal(BigDecimal stateAndLocal) {
        this.stateAndLocal = stateAndLocal;
    }

    public BigDecimal getStructures() {
        return structures;
    }

    public void setStructures(BigDecimal structures) {
        this.structures = structures;
    }

    public BigDecimal getExports() {
        return exports;
    }

    public void setExports(BigDecimal exports) {
        this.exports = exports;
    }

    public BigDecimal getImports() {
        return imports;
    }

    public void setImports(BigDecimal imports) {
        this.imports = imports;
    }

    public BigDecimal getDurableGoods() {
        return durableGoods;
    }

    public void setDurableGoods(BigDecimal durableGoods) {
        this.durableGoods = durableGoods;
    }

    public BigDecimal getNonDurGoods() {
        return nonDurGoods;
    }

    public void setNonDurGoods(BigDecimal nonDurGoods) {
        this.nonDurGoods = nonDurGoods;
    }

    public BigDecimal getPersConsExp() {
        return persConsExp;
    }

    public void setPersConsExp(BigDecimal persConsExp) {
        this.persConsExp = persConsExp;
    }

    public BigDecimal getIntPropProducts() {
        return intPropProducts;
    }

    public void setIntPropProducts(BigDecimal intPropProducts) {
        this.intPropProducts = intPropProducts;
    }

    public BigDecimal getEquipment() {
        return equipment;
    }

    public void setEquipment(BigDecimal equipment) {
        this.equipment = equipment;
    }

    
    
}
