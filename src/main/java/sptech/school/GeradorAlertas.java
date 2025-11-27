package sptech.school;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeradorAlertas {

    private static final Double LIMITE_MAXIMO_ALERTA = 100.0;
    private static final String NOME_ARQUIVO_DADOS_SIMULADO = "trusted.csv"; // Arquivo padrão para Simulado/Local

    private static final int INDICE_MAC_ADRESS = 0;
    private static final int INDICE_DT_HORA = 1;
    private static final int INDICE_ID_MAINFRAME = 2;
    private static final int INDICE_CPU = 3;
    private static final int INDICE_RAM = 4;
    private static final int INDICE_DISCO = 5;


    // gerar json alertas
    public static String gerarJsonAlertas() {
        Dotenv dotenv = Dotenv.load();
        String modoExecucao = dotenv.get("MODO_EXECUCAO", "LOCAL");
        List<String[]> dadosMainframe = null;
        List<Alerta> listaAlertas = new ArrayList<>();

        // rxtract (Leitura do CSV)
        if (modoExecucao.equalsIgnoreCase("AWS")) {
            System.out.println("Lendo dados do bucket TRUSTED (AWS)...");





        } else if (modoExecucao.equalsIgnoreCase("SIMULADO") || modoExecucao.equalsIgnoreCase("LOCAL")) {
            try {
                System.out.println("Lendo arquivo de dados localmente: " + NOME_ARQUIVO_DADOS_SIMULADO);
                dadosMainframe = lerArquivoCsvLocal(NOME_ARQUIVO_DADOS_SIMULADO);
            } catch (IOException e) {
                System.err.println("Erro ao ler arquivo local: " + e.getMessage());
                return "[]";
            }
        }

        if (dadosMainframe == null || dadosMainframe.isEmpty()) {
            System.out.println("Nenhuma linha de dados encontrada para processamento.");
            return "[]";
        }

        // transform and Load (Conexão e Processamento)
        try (Connection conn = ConexaoBd.getConnection()) {

            System.out.println("Conexão com BD estabelecida. Processando dados e gerando alertas...");

            // reutiliza a função que processa os dados, busca limites e insere no BD
            processarDadosParaAlertas(conn, dadosMainframe, listaAlertas);

        } catch (SQLException e) {
            System.err.println("Erro de SQL (conexão ou processamento): " + e.getMessage());
        }

        // output (Montar JSON e Salvar/Exibir)
        String jsonAlertas = montarJsonAlertas(listaAlertas);

        if (modoExecucao.equalsIgnoreCase("SIMULADO") || modoExecucao.equalsIgnoreCase("LOCAL")) {
            System.out.println("\n--- SAÍDA JSON DOS ALERTAS GERADOS (Modo " + modoExecucao.toUpperCase() + ") ---");
            System.out.println(jsonAlertas);
        } else if (modoExecucao.equalsIgnoreCase("AWS")) {
            String nomeArquivoJson = "alertas_" + System.currentTimeMillis() + ".json";
            ConexaoAws.salvarJsonNoS3(nomeArquivoJson, jsonAlertas);
            System.out.println("JSON gerado com sucesso. Lógica de envio para AWS S3 deve ser implementada/descomentada.");
        }

        return jsonAlertas;
    }


    // processa os dados de um mainframe especifico
    public static void processarDadosParaAlertas(Connection conn, List<String[]> dadosMainframe, List<Alerta> listaAlertas) {
        if (dadosMainframe == null || dadosMainframe.isEmpty()) {
            return;
        }

        // agrupa linhas por mainframe
        Map<String, List<String[]>> dadosPorMainframe = new HashMap<>();
        for (String[] linha : dadosMainframe) {
            if (linha.length > INDICE_MAC_ADRESS) {
                String macAdress = linha[INDICE_MAC_ADRESS];
                dadosPorMainframe.computeIfAbsent(macAdress, k -> new ArrayList<>()).add(linha);
            }
        }

        for (Map.Entry<String, List<String[]>> entry : dadosPorMainframe.entrySet()) {
            String macAdress = entry.getKey();

            try {
                // busca os limites no BD
                Map<String, Double[]> limitesMainframe = ConexaoBd.buscarLimitesMetricas(conn, macAdress);

                if (limitesMainframe.isEmpty()) {
                    System.out.println("Limites não encontrados no BD para o MAC: " + macAdress);
                    continue;
                }

                // processa linha por linha do mainframe
                for (String[] linha : entry.getValue()) {
                    processarLinhaMainframe(conn, listaAlertas, linha, limitesMainframe);
                }
            } catch (SQLException e) {
                System.err.println("Erro de SQL ao buscar limites para " + macAdress + ": " + e.getMessage());
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // ------------------------------------------------------------------------------------------------------------------

    // verifica a linha
    private static void processarLinhaMainframe(Connection conn, List<Alerta> listaAlertas, String[] linha,
                                                Map<String, Double[]> limitesMainframe) {

        if (linha.length < INDICE_DISCO + 1) return;

        String dtHora = linha[INDICE_DT_HORA];
        String macAdress = linha[INDICE_MAC_ADRESS];
        String identificacaoMainframe = linha[INDICE_ID_MAINFRAME];

        Map<String, Integer> mapaIndices = new HashMap<>();
        mapaIndices.put("Processador", INDICE_CPU);
        mapaIndices.put("Memória RAM", INDICE_RAM);
        mapaIndices.put("Disco Rígido", INDICE_DISCO);

        for (Map.Entry<String, Integer> entry : mapaIndices.entrySet()) {
            String nomeComponenteBd = entry.getKey();
            int indiceCsv = entry.getValue();

            if (limitesMainframe.containsKey(nomeComponenteBd)) {

                Double limiteMin = limitesMainframe.get(nomeComponenteBd)[0];
                Double limiteMax = limitesMainframe.get(nomeComponenteBd)[1];

                try {
                    Double valorColetado = Double.parseDouble(linha[indiceCsv].replace(",", "."));
                    String gravidade = definirGravidade(valorColetado, limiteMin, limiteMax);

                    if (gravidade != null && !gravidade.equals("Normal")) {
                        listaAlertas.add(new Alerta(dtHora, valorColetado, nomeComponenteBd, gravidade, macAdress, identificacaoMainframe));
                    }

                } catch (NumberFormatException e) {
                    System.err.println("Erro ao converter valor numérico na linha: " + String.join(";", linha));
                }
            }
        }
    }

    // define a gravidade
    private static String definirGravidade(Double valor, Double min, Double max) {
        Double limiteMuitoUrgenteMax = max + ((LIMITE_MAXIMO_ALERTA - max) / 2);
        Double limiteMuitoUrgenteMin = min / 2;

        if (valor >= LIMITE_MAXIMO_ALERTA || valor <= 0.00) return "Emergencia";
        else if (valor >= limiteMuitoUrgenteMax || valor <= limiteMuitoUrgenteMin) return "Muito Urgente";
        else if (valor > max || valor < min) return "Urgente";
        return "Normal";
    }

    // monta o json
    public static String montarJsonAlertas(List<Alerta> listaAlertas) {
        StringBuilder sb = new StringBuilder();
        sb.append("[\n");

        for (int i = 0; i < listaAlertas.size(); i++) {
            sb.append(listaAlertas.get(i).toString());

            if (i < listaAlertas.size() - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n");
            }
        }

        sb.append("]");
        return sb.toString();
    }

    // leitura local teste
    public static List<String[]> lerArquivoCsvLocal(String nomeArquivo) throws IOException {
        List<String[]> linhas = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(nomeArquivo))) {
            String linha;
            boolean primeiraLinha = true;
            while ((linha = reader.readLine()) != null) {
                if (primeiraLinha) {
                    primeiraLinha = false;
                    continue;
                }
                linhas.add(linha.split(";"));
            }
        }
        return linhas;
    }
}
