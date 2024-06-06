package com.example.bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StaticDataLoader {
    private static final String FILE_PATH = "data/symbols_valid_meta.csv";

    public static Map<String, String> loadStaticData() {
        Map<String, String> symbolMap = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            // Pomijanie nagłówka
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                String symbol = fields[1]; // Zakładając, że Symbol jest w drugiej kolumnie
                String securityName = fields[2]; // Zakładając, że Security Name jest w trzeciej kolumnie
                symbolMap.put(symbol, securityName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return symbolMap;
    }
}
