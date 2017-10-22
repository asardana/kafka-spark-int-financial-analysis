package com.financial.kafka.spark.integration;

import java.io.Serializable;

/**
 * Created by Aman on 10/14/2017.
 */
public final class LoanDataRecord implements Serializable{

    private String loanAmt;
    private String fundedAmt;
    private String term;
    private String intRate;
    private String grade;
    private String homeOwnership;
    private String annualIncome;
    private String addressState;
    private String policyCode;

    public String getLoanAmt() {
        return loanAmt;
    }

    public void setLoanAmt(String loanAmt) {
        this.loanAmt = loanAmt;
    }

    public String getFundedAmt() {
        return fundedAmt;
    }

    public void setFundedAmt(String fundedAmt) {
        this.fundedAmt = fundedAmt;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public String getIntRate() {
        return intRate;
    }

    public void setIntRate(String intRate) {
        this.intRate = intRate;
    }

    public String getGrade() {
        return grade;
    }

    public void setGrade(String grade) {
        this.grade = grade;
    }

    public String getHomeOwnership() {
        return homeOwnership;
    }

    public void setHomeOwnership(String homeOwnership) {
        this.homeOwnership = homeOwnership;
    }

    public String getAnnualIncome() {
        return annualIncome;
    }

    public void setAnnualIncome(String annualIncome) {
        this.annualIncome = annualIncome;
    }

    public String getAddressState() {
        return addressState;
    }

    public void setAddressState(String addressState) {
        this.addressState = addressState;
    }

    public String getPolicyCode() {
        return policyCode;
    }

    public void setPolicyCode(String policyCode) {
        this.policyCode = policyCode;
    }

    @Override
    public String toString() {
        return "LoanDataRecord{" +
                "loanAmt='" + loanAmt + '\'' +
                ", fundedAmt='" + fundedAmt + '\'' +
                ", term='" + term + '\'' +
                ", intRate='" + intRate + '\'' +
                ", grade='" + grade + '\'' +
                ", homeOwnership='" + homeOwnership + '\'' +
                ", annualIncome='" + annualIncome + '\'' +
                ", addressState='" + addressState + '\'' +
                ", policyCode='" + policyCode + '\'' +
                '}';
    }
}
