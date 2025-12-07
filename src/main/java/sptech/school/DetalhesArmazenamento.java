package sptech.school;

import io.github.cdimascio.dotenv.Dotenv;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.io.FileWriter;
import java.io.IOException;

import java.util.*;

public class DetalhesArmazenamento {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) {
        new DetalhesArmazenamento().executar();
    }

    public void executar() {

        List<String> idEmpresas = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(
                Dotenv.load().get("DB_URL"),
                Dotenv.load().get("DB_USER"),
                Dotenv.load().get("DB_PASSWORD"))) {

            idEmpresas = ConexaoBd.listaEmpresas(conn);

        } catch (SQLException e) {
            System.err.println("erro ao conectar no banco: " + e.getMessage());
        }

        // Lista para consolidar todas as empresas
        List<Map<String, Object>> todasAsEmpresas = new ArrayList<>();

        for (String empresa : idEmpresas) {

            List<Map<String, Object>> listaMainframes = new ArrayList<>();

            List<String> dirs = ConexaoAws.listarDiretorios(empresa);

            System.out.println("Processando empresa: " + empresa + " | Diretórios encontrados: " + dirs.size());

            for (String dir : dirs) {

                String prefixo = empresa + "/";
                String mac = dir.startsWith(prefixo) ? dir.substring(prefixo.length()).replace("/", "") : dir.replace("1/", "").replace("/", "");

                List<String[]> linhas = ConexaoAws.lerArquivoGeralCsvDoTrusted(empresa, mac);
                String fonte = "GERAL";

                if (linhas == null || linhas.isEmpty()) {
                    linhas = ConexaoAws.lerArquivoCsvDoTrustedDiario(empresa, mac, "trusted.csv");
                    fonte = "DIARIO";
                }

                if (linhas == null || linhas.isEmpty()) {
                    linhas = ConexaoAws.lerArquivoCsvDoTrusted(mac, empresa, "trusted.csv");
                    fonte = "PADRAO";
                }

                int linhasCount = linhas == null ? 0 : linhas.size();
                System.out.println("  MAC: " + mac + " | Fonte: " + fonte + " | Linhas lidas: " + linhasCount);

                if (linhas.isEmpty()) {
                    System.out.println("    AVISO: Nenhuma linha encontrada para MAC " + mac);
                    continue;
                }

                String primeiraCelula = linhas.get(0).length > 0 ? linhas.get(0)[0].toLowerCase() : "";
                if (primeiraCelula.equalsIgnoreCase("macadress") ||
                        primeiraCelula.equalsIgnoreCase("mac") ||
                        (primeiraCelula.equalsIgnoreCase("dt_hora") || primeiraCelula.equalsIgnoreCase("hora"))) {
                    System.out.println("    Info: cabeçalho detectado e removido para MAC " + mac + " (" + primeiraCelula + ")");
                    linhas.remove(0);
                }

                if (linhas.isEmpty()) {
                    System.out.println("    AVISO: Após remover cabeçalho, não há linhas para MAC " + mac);
                    continue;
                }

                int ultimo = linhas.size() - 1;

                if (linhas.get(ultimo).length < 9) {
                    System.out.println("    AVISO: Última linha incompleta para MAC " + mac + " (células encontradas: " + linhas.get(ultimo).length + ")");
                    continue;
                }

                String macAdress = linhas.get(ultimo)[0];
                Double usoPct = parseDouble(linhas.get(ultimo)[3]);
                Double throughput = parseDouble(linhas.get(ultimo)[4]);
                Integer iopsTotal = parseInt(linhas.get(ultimo)[5]);
                Integer iopsLeitura = parseInt(linhas.get(ultimo)[6]);
                Integer iopsEscrita = parseInt(linhas.get(ultimo)[7]);
                Double latencia = parseDouble(linhas.get(ultimo)[8]);

                List<Double> histDisco = new ArrayList<>();
                List<Double> histLatencia = new ArrayList<>();
                List<Integer> histIops = new ArrayList<>();
                List<String> histDatas = new ArrayList<>();

                int idx = ultimo;
                for (int i = 0; i < 5 && idx >= 0; i++, idx--) {
                    histDisco.add(parseDouble(linhas.get(idx)[3]));
                    histLatencia.add(parseDouble(linhas.get(idx)[8]));
                    histIops.add(parseInt(linhas.get(idx)[5]));
                    histDatas.add(linhas.get(idx)[1]);
                }

                String fabricante = "";
                String modelo = "";

                Double min = 0.0;
                Double max = 9999.0;

                try (Connection conn = DriverManager.getConnection(
                        Dotenv.load().get("DB_URL"),
                        Dotenv.load().get("DB_USER"),
                        Dotenv.load().get("DB_PASSWORD"))) {

                    Map<String, Object> info = ConexaoBd.buscarDadosMainframe(conn, macAdress);
                    fabricante = info.getOrDefault("fabricante", "").toString();
                    modelo = info.getOrDefault("modelo", "").toString();

                    // Pegar min/max
                    List<String> minMax = ConexaoBd.buscarMinMax(conn, macAdress);
                    if (minMax != null && minMax.size() >= 2) {
                        try { min = Double.parseDouble(minMax.get(0)); } catch (Exception ex) { min = 0.0; }
                        try { max = Double.parseDouble(minMax.get(1)); } catch (Exception ex) { max = 9999.0; }
                    }

                } catch (SQLException e) {
                    System.err.println("erro consultando banco: " + e.getMessage());
                }

                boolean alerta = (usoPct < min || usoPct > max);

                Double crescimentoDiario = calcAumento(histDisco);
                Double crescimento30dias = calc30dias(linhas);

                Double diasAte95;
                if (crescimentoDiario > 0) {
                    diasAte95 = (95.0 - usoPct) / crescimentoDiario;
                    if (diasAte95.isInfinite() || diasAte95.isNaN()) {
                        diasAte95 = null;
                    }
                } else {
                    diasAte95 = null;
                }

                Integer picoIops = Collections.max(histIops);
                Double picoLat = Collections.max(histLatencia);

                Map<String, Object> mf = new HashMap<>();

                mf.put("mac", macAdress);
                mf.put("fabricante", fabricante);
                mf.put("modelo", modelo);

                mf.put("uso_disco_percentual", usoPct);
                mf.put("throughput_mbs", throughput);
                mf.put("iops_total", iopsTotal);
                mf.put("iops_leitura", iopsLeitura);
                mf.put("iops_escrita", iopsEscrita);
                mf.put("latencia_ms", latencia);

                mf.put("historico_disco", histDisco);
                mf.put("historico_latencia", histLatencia);
                mf.put("historico_iops", histIops);
                mf.put("historico_datas", histDatas);

                mf.put("crescimento_diario_pct", crescimentoDiario);
                mf.put("crescimento_30dias_pct", crescimento30dias);
                mf.put("dias_ate_95pct", diasAte95);
                mf.put("pico_iops", picoIops);
                mf.put("pico_latencia", picoLat);

                mf.put("min", min);
                mf.put("max", max);
                mf.put("alerta", alerta);

                listaMainframes.add(mf);
            }

            Map<String, Object> empresaJson = new HashMap<>();
            empresaJson.put("empresa", empresa);
            empresaJson.put("mainframes", listaMainframes);

            todasAsEmpresas.add(empresaJson);
        }

        Map<String, Object> jsonConsolidado = new HashMap<>();
        jsonConsolidado.put("empresas", todasAsEmpresas);

        String json = gson.toJson(jsonConsolidado);
        String nomeArq = "detalhesArmazenamento.json";

        try (FileWriter writer = new FileWriter(nomeArq)) {
            writer.write(json);
            System.out.println("JSON consolidado salvo em: " + nomeArq);
        } catch (IOException e) {
            System.err.println("erro salvando JSON local: " + e.getMessage());
        }

        // Salvar no S3
        ConexaoAws.salvarJsonNoS3(nomeArq, json);
        System.out.println("JSON consolidado salvo no S3: " + nomeArq);
    }

    private static Double parseDouble(String v) {
        try { return Double.parseDouble(v); }
        catch (Exception e) { return 0.0; }
    }

    private static Integer parseInt(String v) {
        try { return Integer.parseInt(v); }
        catch (Exception e) { return 0; }
    }

    private static Double calcAumento(List<Double> hist) {
        if (hist.size() < 2) return 0.0;

        double dif = hist.get(0) - hist.get(hist.size() - 1);
        return dif / (hist.size() - 1);
    }

    private static Double calc30dias(List<String[]> linhas) {
        if (linhas.size() < 30) return 0.0;

        double inicio = parseDouble(linhas.get(linhas.size() - 30)[3]);
        double fim = parseDouble(linhas.get(linhas.size() - 1)[3]);

        return fim - inicio;
    }
}
