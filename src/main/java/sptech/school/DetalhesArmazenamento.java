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

        List<String> idEmpresas = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(
                Dotenv.load().get("DB_URL"),
                Dotenv.load().get("DB_USER"),
                Dotenv.load().get("DB_PASSWORD"))) {

            idEmpresas = ConexaoBd.listaEmpresas(conn);

        } catch (SQLException e) {
            System.err.println("Erro ao conectar no banco: " + e.getMessage());
        }

        List<Map<String, Object>> empresasJson = new ArrayList<>();

        // PROCESSAMENTO DE CADA EMPRESA
        for (String empresa : idEmpresas) {

            List<Map<String, Object>> listaMainframes = new ArrayList<>();

            List<String> dirs = ConexaoAws.listarDiretorios(empresa);

            for (String dir : dirs) {

                String mac = dir.replace("1/", "").replace("/", "");
                List<String[]> linhas = ConexaoAws.lerArquivoCsvDoTrusted(mac, empresa, "trusted.csv");

                if (linhas.isEmpty()) continue;

                int ultimo = linhas.size() - 1;

                // DADOS ATUAIS
                String macAdress = linhas.get(ultimo)[0];
                Double usoPct = parseDouble(linhas.get(ultimo)[3]);
                Double throughput = parseDouble(linhas.get(ultimo)[4]);
                Integer iopsTotal = parseInt(linhas.get(ultimo)[5]);
                Integer iopsLeitura = parseInt(linhas.get(ultimo)[6]);
                Integer iopsEscrita = parseInt(linhas.get(ultimo)[7]);
                Double latencia = parseDouble(linhas.get(ultimo)[8]);

                // ====== HISTÓRICOS (últimos 5) ======
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

                // BANCO
                String fabricante = "";
                String modelo = "";

                try (Connection conn = DriverManager.getConnection(
                        Dotenv.load().get("DB_URL"),
                        Dotenv.load().get("DB_USER"),
                        Dotenv.load().get("DB_PASSWORD"))) {

                    Map<String, Object> info = ConexaoBd.buscarDadosMainframe(conn, macAdress);

                    fabricante = info.getOrDefault("fabricante", "").toString();
                    modelo = info.getOrDefault("modelo", "").toString();

                } catch (SQLException e) {
                    System.err.println("Erro consultando banco: " + e.getMessage());
                }

                //CÁLCULOS EM %
                Double crescimentoDiario = calcAumento(histDisco);
                Double crescimento30dias = calc30dias(linhas);

                Double diasAte95 = null;
                if (crescimentoDiario > 0) {
                    diasAte95 = (95.0 - usoPct) / crescimentoDiario;
                }

                Integer picoIops = Collections.max(histIops);
                Double picoLat = Collections.max(histLatencia);

                // JSON DO MAINFRAME
                Map<String, Object> mf = new HashMap<>();
                mf.put("mac", macAdress);
                mf.put("fabricante", fabricante);
                mf.put("modelo", modelo);

                // métricas atuais
                mf.put("uso_disco_percentual", usoPct);
                mf.put("throughput_mbs", throughput);
                mf.put("iops_total", iopsTotal);
                mf.put("iops_leitura", iopsLeitura);
                mf.put("iops_escrita", iopsEscrita);
                mf.put("latencia_ms", latencia);

                // historicos
                mf.put("historico_disco", histDisco);
                mf.put("historico_latencia", histLatencia);
                mf.put("historico_iops", histIops);
                mf.put("historico_datas", histDatas);

                // anAlises
                mf.put("crescimento_diario_pct", crescimentoDiario);
                mf.put("crescimento_30dias_pct", crescimento30dias);
                mf.put("dias_ate_95pct", diasAte95);
                mf.put("pico_iops", picoIops);
                mf.put("pico_latencia", picoLat);

                listaMainframes.add(mf);
            }

            Map<String, Object> empresaJson = new HashMap<>();
            empresaJson.put("empresa", empresa);
            empresaJson.put("mainframes", listaMainframes);

            empresasJson.add(empresaJson);
        }

        // SALVAR JSON
        String json = gson.toJson(empresasJson);

        try (FileWriter writer = new FileWriter("detalhesArmazenamento.json")) {
            writer.write(json);
        } catch (IOException e) {
            System.err.println("Erro salvando JSON local: " + e.getMessage());
        }

        ConexaoAws.salvarJsonNoS3("detalhesArmazenamento.json", json);
    }

    //FUNÇÕES

    private static Double parseDouble(String v) {
        try { return Double.valueOf(v.replace(",", ".")); }
        catch (Exception e) { return 0.0; }
    }

    private static Integer parseInt(String v) {
        try { return Integer.valueOf(v.replaceAll("[^0-9]", "")); }
        catch (Exception e) { return 0; }
    }

    private static Double calcAumento(List<Double> hist) {
        if (hist.size() < 2) return 0.0;
        return hist.get(0) - hist.get(hist.size() - 1);
    }

    private static Double calc30dias(List<String[]> linhas) {
        if (linhas.size() < 30) return 0.0;
        double inicio = parseDouble(linhas.get(linhas.size() - 30)[3]);
        double atual = parseDouble(linhas.get(linhas.size() - 1)[3]);
        return atual - inicio;
    }
}
