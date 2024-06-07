package com.example.bigdata;

public class Aggregation {
    private double sumClose;
    private double minLow;
    private double maxHigh;
    private double sumVolume;
    private int count;
    private double avgClose;

    public Aggregation() {
        this.sumClose = 0.0;
        this.minLow = Double.MAX_VALUE;
        this.maxHigh = Double.MIN_VALUE;
        this.sumVolume = 0;
        this.count = 0;
        this.avgClose = 0.0;
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

