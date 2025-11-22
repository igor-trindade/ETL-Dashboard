package sptech.school;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.List;
import java.util.Map;

public class TrataCsv {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    /**
     * Converte vários mainframes em uma LISTA JSON
     *
     * Entrada esperada:
     * List<Map<String, Object>> listaMainframes
     *
     * Saída:
     * [
     *   { ... },
     *   { ... },
     *   { ... }
     * ]
     */
    public static String converterListaMainframes(List<Map<String, Object>> listaMainframes) {

        if (listaMainframes == null || listaMainframes.isEmpty()) {
            throw new RuntimeException("Lista de mainframes vazia");
        }

        return gson.toJson(listaMainframes);
    }



}
