package com.example.bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

// Load static dataset into a hashmap (symbol -> security name)
public class StaticDatasetMapper {
    private static final String path = "data/symbols_valid_meta.csv";
    public static Map<String, String> loadAndMap() {
        Map<String, String> symbolToName = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line; reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] col = line.split(",");
                symbolToName.put(col[1], col[2]); // symbol -> security name
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return symbolToName;
    }
}
