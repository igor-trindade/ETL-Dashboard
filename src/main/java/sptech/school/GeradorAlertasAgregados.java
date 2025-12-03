package sptech.school;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeradorAlertasAgregados {

    // Constantes de Índice copiadas de GeradorAlertas
    private static final int INDICE_MAC_ADRESS = 0;
    private static final int INDICE_DT_HORA = 1;
    private static final int INDICE_ID_MAINFRAME = 2;
    private static final int INDICE_CPU = 3;
    private static final int INDICE_RAM = 4;
    private static final int INDICE_DISCO = 5;

    // Constante de Limite de Alerta copiada de GeradorAlertas
    private static final Double LIMITE_MAXIMO_ALERTA = 100.0; // Adicionada esta linha

    public static Map<String, AlertaQuantidade> processarDadosParaContagem(
            Connection conn,
            List<String[]> dadosMainframe
    ) {
        // ... (Corpo da função processarDadosParaContagem permanece o mesmo)
        Map<String, AlertaQuantidade> mapaContagemAlertas = new HashMap<>();

        if (dadosMainframe == null || dadosMainframe.isEmpty()) {
            return mapaContagemAlertas;
        }

        // 1. Agrupar linhas por MAC Adress
        Map<String, List<String[]>> dadosPorMainframe = new HashMap<>();
        for (String[] linha : dadosMainframe) {
            if (linha.length > INDICE_MAC_ADRESS) {
                String macAdress = linha[INDICE_MAC_ADRESS];
                dadosPorMainframe.computeIfAbsent(macAdress, k -> new ArrayList<>()).add(linha);
            }
        }

        // 2. Processar Mainframes individualmente
        for (Map.Entry<String, List<String[]>> entry : dadosPorMainframe.entrySet()) {
            String macAdress = entry.getKey();

            try {
                Map<String, MetricaInfo> limitesMainframe = ConexaoBd.buscarLimitesMetricas(conn, macAdress);

                if (limitesMainframe.isEmpty()) {
                    continue;
                }

                // Processa linha por linha do mainframe, populando o mapa de contagem
                for (String[] linha : entry.getValue()) {
                    processarLinha(conn, linha, limitesMainframe, mapaContagemAlertas);
                }
            } catch (SQLException e) {
                System.err.println(" Erro de SQL ao buscar limites para " + macAdress + ": " + e.getMessage());
            }
        }

        return mapaContagemAlertas;
    }


    private static void processarLinha(
            Connection conn,
            String[] linha,
            Map<String, MetricaInfo> limitesMainframe,
            Map<String, AlertaQuantidade> mapaContagemAlertas
    ) {

        if (linha.length < INDICE_DISCO + 1) return;

        String dtHora = linha[INDICE_DT_HORA];
        String macAdress = linha[INDICE_MAC_ADRESS];

        Map<String, Integer> mapaIndices = new HashMap<>();
        mapaIndices.put("Processador", INDICE_CPU);
        mapaIndices.put("Memória RAM", INDICE_RAM);
        mapaIndices.put("Disco Rígido", INDICE_DISCO);

        for (Map.Entry<String, Integer> entry : mapaIndices.entrySet()) {
            String componente = entry.getKey();
            int indiceCsv = entry.getValue();

            if (!limitesMainframe.containsKey(componente)) continue;

            MetricaInfo mi = limitesMainframe.get(componente);

            try {
                Double valor = Double.parseDouble(linha[indiceCsv].replace(",", "."));

                // 1. Determina a Gravidade - AGORA CHAMANDO O MÉTODO LOCAL
                String gravidade = definirGravidade(valor, mi.getMin(), mi.getMax());

                // Se houver alerta, insere no BD e incrementa a contagem para o JSON
                if (!gravidade.equals("Normal")) {
                    ConexaoBd.inserirAlerta(conn, dtHora, valor, mi.getIdMetrica());

                    String chave = macAdress + "|" + componente + "|" + gravidade;

                    AlertaQuantidade contagem = mapaContagemAlertas.get(chave);
                    if (contagem == null) {
                        contagem = new AlertaQuantidade(macAdress, componente, gravidade);
                        mapaContagemAlertas.put(chave, contagem);
                    }
                    contagem.incrementarQtd();
                }

            } catch (Exception e) {
                System.err.println("Erro ao converter valor numérico: " + String.join(";", linha));
            }
        }
    }

    // =================================================================
    // MÉTODO DEFINIR GRAVIDADE (COPIADO E TORNADO PRIVADO NESTA CLASSE)
    // =================================================================

    private static String definirGravidade(Double valor, Double min, Double max) {

        Double limiteMuitoUrgenteMax = max + ((LIMITE_MAXIMO_ALERTA - max) / 2);
        Double limiteMuitoUrgenteMin = min / 2;

        if (valor >= LIMITE_MAXIMO_ALERTA || valor <= 0.00) return "Emergência";
        else if (valor >= limiteMuitoUrgenteMax || valor <= limiteMuitoUrgenteMin) return "Muito Urgente";
        else if (valor > max || valor < min) return "Urgente";
        return "Normal";
    }

    // =================================================================

    /**
     * Monta o JSON a partir do mapa de contagens agregadas.
     * ... (documentação)
     */
    public static String montarJsonAlertasAgregados(Map<String, AlertaQuantidade> mapaContagemAlertas) {
        List<AlertaQuantidade> listaAgregada = new ArrayList<>(mapaContagemAlertas.values());

        StringBuilder sb = new StringBuilder();
        sb.append("[\n");

        for (int i = 0; i < listaAgregada.size(); i++) {
            sb.append(listaAgregada.get(i).toString());

            if (i < listaAgregada.size() - 1) {
                sb.append(",\n");
            } else {
                sb.append("\n");
            }
        }

        sb.append("]");
        return sb.toString();
    }
}