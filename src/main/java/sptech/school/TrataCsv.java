package sptech.school;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.*;

public class TrataCsv {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    // CONVERTER LISTA CSV → JSON
    public static String converterCsvParaJson(List<String[]> linhasCsv) {

        if (linhasCsv.isEmpty()) {
            throw new RuntimeException("CSV vazio");
        }

        String[] header = linhasCsv.get(0);
        List<Map<String, String>> registros = new ArrayList<>();

        for (int i = 1; i < linhasCsv.size(); i++) {

            String[] valores = linhasCsv.get(i);
            Map<String, String> map = new LinkedHashMap<>();

            for (int j = 0; j < header.length && j < valores.length; j++) {
                map.put(header[j], valores[j]);
            }

            registros.add(map);
        }

        return gson.toJson(registros);
    }
}
