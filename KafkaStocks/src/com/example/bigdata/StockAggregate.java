package com.example.bigdata;

import java.io.Serializable;

public class StockAggregate implements Serializable {
    private double sumClose;
    private double minLow;
    private double maxHigh;
    private double sumVolume;
    private int count;

    public StockAggregate() {
        this.minLow = Double.MAX_VALUE;
        this.maxHigh = Double.MIN_VALUE;
    }

    public StockAggregate add(StockDataRecord record) {
        this.sumClose += Double.parseDouble(record.getClose());
        this.minLow = Math.min(this.minLow, Double.parseDouble(record.getLow()));
        this.maxHigh = Math.max(this.maxHigh, Double.parseDouble(record.getHigh()));
        this.sumVolume += Double.parseDouble(record.getVolume());
        this.count++;
        return this;
    }

    public double getAvgClose() {
        return this.count > 0 ? this.sumClose / this.count : 0;
    }

    public double getMinLow() {
        return this.minLow;
    }

    public double getMaxHigh() {
        return this.maxHigh;
    }

    public double getSumVolume() {
        return this.sumVolume;
    }

    public int getCount() {
        return this.count;
    }

    public void setSumClose(double sumClose) {
        this.sumClose = sumClose;
    }

    public void setMinLow(double minLow) {
        this.minLow = minLow;
    }

    public void setMaxHigh(double maxHigh) {
        this.maxHigh = maxHigh;
    }

    public void setSumVolume(double sumVolume) {
        this.sumVolume = sumVolume;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public boolean isAnomaly(double P) {
        return (this.maxHigh - this.minLow) / this.maxHigh > P / 100.0;
    }

    public double getPriceFluctuationRatio() {
        return (this.maxHigh - this.minLow) / this.maxHigh;
    }

    public double getMaxPrice() {
        return this.maxHigh;
    }

    public double getMinPrice() {
        return this.minLow;
    }
}
