package com.example.bigdata;

public class Aggregation {
    private double sumClose;
    private double minLow;
    private double maxHigh;
    private double sumVolume;
    private int count;
    private double avgClose;
    private String stockSymbol;
    private String stockName;
    private int year;
    private int month;

    public Aggregation() {
        this.sumClose = 0.0;
        this.minLow = Double.MAX_VALUE;
        this.maxHigh = Double.MIN_VALUE;
        this.sumVolume = 0;
        this.count = 0;
        this.avgClose = 0.0;
        this.stockSymbol = null;
        this.stockName = null;
        this.year = 0;
        this.month = 0;
    }

    public Aggregation add(StockData data) {
        this.sumClose += data.getClose();
        this.minLow = Math.min(this.minLow, data.getLow());
        this.maxHigh = Math.max(this.maxHigh, data.getHigh());
        this.sumVolume += data.getVolume();
        this.count++;
        this.avgClose = this.sumClose / this.count;
        return this;
    }

    public boolean isAnomaly(double threshold) {
        double ratio = (this.maxHigh - this.minLow) / this.maxHigh;
        return ratio > threshold;
    }

    public double getSumClose() {
        return sumClose;
    }
    public double getMinLow() {
        return minLow;
    }
    public double getMaxHigh() {
        return maxHigh;
    }
    public double getSumVolume() {
        return sumVolume;
    }
    public int getCount() {
        return count;
    }
    public double getAvgClose() {
        return avgClose;
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
    public void setAvgClose(double avgClose) {
        this.avgClose = avgClose;
    }
    public void  setStockSymbol(String stockSymbol) {
        this.stockSymbol = stockSymbol;
    }
    public void setStockName(String stockName) {
        this.stockName = stockName;
    }
    public void setYear(int year) {
        this.year = year;
    }
    public void setMonth(int month) {
        this.month = month;
    }


    @Override
    public String toString() {
        return "Aggregation{" +
                "avgClose=" + avgClose +
                ", minLow=" + minLow +
                ", maxHigh=" + maxHigh +
                ", sumVolume=" + sumVolume +
                ", count=" + count + // only for debugging
                '}';
    }
}

