package com.example.bigdata;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StockDataRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger("Access");
    // Example log line:
    // 1962-01-02T00:00:00.000Z,0.0,7.6875,7.541666507720947,7.583333492279053,0.9789445996284485,49200.0,FL
    private static final String LOG_ENTRY_PATTERN =
            // 1:data 2:open 3:high 4:close 5:adjClose 6:volume 7:stock
            "^(\\S+),(\\S+),(\\S+),(\\S+),(\\S+),(\\S+),(\\S+),(\\S+)$";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    private String date;
    private String open;
    private String high;
    private String low;
    private String close;
    private String adjClose;
    private String volume;
    private String stock;

    StockDataRecord(String date, String open, String high, String low,
                    String close, String adjClose, String volume,
                    String stock) {
        this.date = date;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.adjClose = adjClose;
        this.volume = volume;
        this.stock = stock;
    }

    public static StockDataRecord parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);
        if (!m.find()) {
            logger.log(Level.ALL, "Cannot parse logline" + logline);
            throw new RuntimeException("Error parsing logline: " + logline);
        }

        return new StockDataRecord(m.group(1), m.group(2), m.group(3), m.group(4),
                m.group(5), m.group(6), m.group(7), m.group(8));
    }

    public static boolean lineIsCorrect(String logline) {
        Matcher m = PATTERN.matcher(logline);
        return m.find();
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getOpen() {
        return open;
    }

    public void setOpen(String open) {
        this.open = open;
    }

    public String getHigh() {
        return high;
    }

    public void setHigh(String high) {
        this.high = high;
    }

    public String getClose() {
        return close;
    }

    public void setClose(String close) {
        this.close = close;
    }

    public String getAdjClose() {
        return adjClose;
    }

    public void setAdjClose(String adjClose) {
        this.adjClose = adjClose;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getStock() {
        return stock;
    }

    public void setStock(String stock) {
        this.stock = stock;
    }

    public String getLow() {return low;}

    public void setLow(String low) {this.low = low;}

    @Override
    public String toString() {
        return String.format("%s %s %s %s %s %s %s %s", date, open, high, low,
                close, adjClose, volume, stock);
    }

    public long getTimestampInMillis() {
        //1962-01-02T00:00:00.000Z
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date;
        try {
            date = sdf.parse(close);
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }
}
