package sptech.school;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DashboardDataProcessor {

    private static final String NOME_ARQUIVO_DADOS = "trusted.csv";
    private static final String ARQUIVO_SAIDA_JSON = "dashboard_data.json";
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    // Índices das colunas no CSV
    private static final int INDICE_MAC_ADRESS = 0;
    private static final int INDICE_TIMESTAMP = 1;
    private static final int INDICE_ID_MAINFRAME = 2;
    private static final int INDICE_USO_CPU_TOTAL = 3;
    private static final int INDICE_USO_RAM_TOTAL = 4;
    private static final int INDICE_USO_DISCO_TOTAL = 5;
    private static final int INDICE_DISCO_THROUGHPUT = 6;
    private static final int INDICE_DISCO_IOPS = 7;
    private static final int INDICE_DISCO_READ_COUNT = 8;
    private static final int INDICE_DISCO_WRITE_COUNT = 9;
    private static final int INDICE_DISCO_LATENCIA = 10;
    private static final int INDICE_PROCESSOS_INICIO = 11; // Início dos dados de processos (nome1, cpu_perc1, mem_perc1)
    private static final int NUM_PROCESSOS = 9;
    private static final int DADOS_POR_PROCESSO = 3; // nome, cpu_perc, mem_perc

    public static void main(String[] args) {
        System.out.println("Iniciando processamento de dados para o Dashboard...");

        List<String[]> dadosMainframe = null;

        try {
            dadosMainframe = lerArquivoCsvLocal(NOME_ARQUIVO_DADOS);
        } catch (IOException e) {
            System.err.println(" Erro ao ler arquivo local: " + e.getMessage());
            return;
        }

        if (dadosMainframe == null || dadosMainframe.isEmpty()) {
            System.out.println("Nenhum dado encontrado no CSV.");
            return;
        }

        List<Map<String, Object>> dadosProcessados = processarDados(dadosMainframe);
        String json = gerarJson(dadosProcessados);
        salvarJsonLocal(json, ARQUIVO_SAIDA_JSON);
        ConexaoAws.salvarJsonNoS3(ARQUIVO_SAIDA_JSON, json);
    }
    public void executar(){
        System.out.println("Iniciando processamento de dados para o Dashboard...");

        List<String[]> dadosMainframe = null;

        try {
            dadosMainframe = lerArquivoCsvLocal(NOME_ARQUIVO_DADOS);
        } catch (IOException e) {
            System.err.println(" Erro ao ler arquivo local: " + e.getMessage());
            return;
        }

        if (dadosMainframe == null || dadosMainframe.isEmpty()) {
            System.out.println("Nenhum dado encontrado no CSV.");
            return;
        }

        List<Map<String, Object>> dadosProcessados = processarDados(dadosMainframe);
        String json = gerarJson(dadosProcessados);
        salvarJsonLocal(json, ARQUIVO_SAIDA_JSON);
        ConexaoAws.salvarJsonNoS3(ARQUIVO_SAIDA_JSON, json);
    }
    private static List<String[]> lerArquivoCsvLocal(String caminhoArquivo) throws IOException {
        List<String[]> dados = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(caminhoArquivo))) {
            String linha;
            boolean primeiraLinha = true;
            while ((linha = br.readLine()) != null) {
                if (primeiraLinha) {
                    primeiraLinha = false; // Ignora o cabeçalho
                    continue;
                }
                // Assume que o CSV usa ';' como delimitador
                dados.add(linha.split(";"));
            }
        }
        return dados;
    }

    private static List<Map<String, Object>> processarDados(List<String[]> dados) {
        List<Map<String, Object>> listaDados = new ArrayList<>();

        for (String[] linha : dados) {
            if (linha.length < INDICE_PROCESSOS_INICIO + (NUM_PROCESSOS * DADOS_POR_PROCESSO)) {
                System.err.println("Linha ignorada: número insuficiente de colunas.");
                continue;
            }

            Map<String, Object> registro = new HashMap<>();
            registro.put("macAdress", linha[INDICE_MAC_ADRESS]);
            registro.put("timestamp", linha[INDICE_TIMESTAMP]);
            registro.put("identificacao_mainframe", linha[INDICE_ID_MAINFRAME]);
            registro.put("uso_cpu_total_perc", linha[INDICE_USO_CPU_TOTAL].replace(",", "."));
            registro.put("uso_ram_total_perc", linha[INDICE_USO_RAM_TOTAL].replace(",", "."));
            registro.put("uso_disco_total_perc", linha[INDICE_USO_DISCO_TOTAL].replace(",", "."));
            registro.put("disco_throughput_mbs", linha[INDICE_DISCO_THROUGHPUT].replace(",", "."));
            registro.put("disco_iops_total", linha[INDICE_DISCO_IOPS].replace(",", "."));
            registro.put("disco_read_count", linha[INDICE_DISCO_READ_COUNT]);
            registro.put("disco_write_count", linha[INDICE_DISCO_WRITE_COUNT]);
            registro.put("disco_latencia_ms", linha[INDICE_DISCO_LATENCIA].replace(",", "."));

            List<Map<String, String>> processos = new ArrayList<>();
            for (int i = 0; i < NUM_PROCESSOS; i++) {
                int baseIndex = INDICE_PROCESSOS_INICIO + (i * DADOS_POR_PROCESSO);
                Map<String, String> processo = new HashMap<>();
                processo.put("nome", linha[baseIndex]);
                processo.put("cpu_perc", linha[baseIndex + 1].replace(",", "."));
                processo.put("mem_perc", linha[baseIndex + 2].replace(",", "."));
                processos.add(processo);
            }
            registro.put("processos", processos);
            listaDados.add(registro);
        }
        return listaDados;
    }

    private static String gerarJson(List<Map<String, Object>> listaDados) {
        return gson.toJson(listaDados);
    }

    private static void salvarJsonLocal(String json, String caminhoArquivo) {
        try (FileWriter writer = new FileWriter(caminhoArquivo)) {
            writer.write(json);
            System.out.println("JSON de dados do Dashboard salvo em: " + caminhoArquivo);
        } catch (IOException e) {
            System.err.println("Erro ao salvar JSON: " + e.getMessage());
        }
    }
}