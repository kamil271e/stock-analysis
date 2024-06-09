package com.example.bigdata;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StockData {
    private String date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double adjClose;
    private double volume;
    private String stock;

    public StockData(String date, double open, double high, double low, double close, double adjClose, double volume, String stock) {
        this.date = date;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjClose = adjClose;
        this.volume = volume;
        this.stock = stock;
    }

    // Getters and setters

    public String getStock() {
        return stock;
    }

    public void setStock(String stock) {
        this.stock = stock;
    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getClose() {
        return close;
    }

    public void setClose(double close) {
        this.close = close;
    }

    public double getAdjClose() {
        return adjClose;
    }

    public void setAdjClose(double adjClose) {
        this.adjClose = adjClose;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public LocalDateTime getDate() {
        return LocalDateTime.parse(date, DateTimeFormatter.ISO_DATE_TIME);
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return String.join(";", date, String.valueOf(open), String.valueOf(high), String.valueOf(low), String.valueOf(close), String.valueOf(adjClose), String.valueOf(volume), stock);
    }
}
