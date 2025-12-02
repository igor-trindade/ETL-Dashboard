package sptech.school;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.github.cdimascio.dotenv.Dotenv;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class DashboardProcesso {
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) {

        List<String> idEmpresas = new ArrayList<>();

        List<TrustedCampos> listaTrusted = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(
                Dotenv.load().get("DB_URL"), Dotenv.load().get("DB_USER"), Dotenv.load().get("DB_PASSWORD"))) {
            idEmpresas = ConexaoBd.listaEmpresas(conn);
        } catch (SQLException e) {
            System.err.println("Erro ao conectar no banco: " + e.getMessage());
        }

        //  array de empresas
        List<Map<String, Object>> empresasJson = new ArrayList<>();

        for (String empresa : idEmpresas) {

            List<Map<String, Object>> mainframesJson = new ArrayList<>();

            List<String> dirs = ConexaoAws.listarDiretorios(empresa);
            for (String dir : dirs) {

                String mac = dir.replace("1/", "").replace("/", "");
                //linhas
                List<String[]> linhas = ConexaoAws.lerArquivoCsvDoTrusted(mac, empresa, "trusted.csv");

                if (linhas.isEmpty()) continue;

                // pula o cabeçalho
                for (int i = 1; i < linhas.size(); i++) {
                    String[] registro = linhas.get(i);

                    TrustedCampos trusted = new TrustedCampos();

                    trusted.setMacAdress(registro[0]);
                    trusted.setTimestamp(registro[1]);
                    trusted.setIdentificao_mainframe(registro[2]);

                    trusted.setUso_cpu_total_perc(Double.parseDouble(registro[3].replace(",", ".")));
                    trusted.setUso_ram_total_perc(Double.parseDouble(registro[4].replace(",", ".")));
                    trusted.setUso_disco_total_perc(Double.parseDouble(registro[5].replace(",", ".")));
                    trusted.setDisco_throughput_mbs(Double.parseDouble(registro[6].replace(",", ".")));
                    trusted.setDisco_iops_total(Double.parseDouble(registro[7].replace(",", ".")));
                    trusted.setDisco_read_count(Integer.parseInt(registro[8] ));
                    trusted.setDisco_write_count(Integer.parseInt(registro[9] ));
                    trusted.setDisco_latencia_ms(Double.parseDouble(registro[10].replace(",", ".")));

                    // process 1
                    trusted.setNome1(registro[11]);
                    trusted.setCpu_perc1(Double.parseDouble(registro[12].replace(",", ".")));
                    trusted.setMem_perc1(Double.parseDouble(registro[13].replace(",", ".")));

                    // process 2
                    trusted.setNome2(registro[14]);
                    trusted.setCpu_perc2(Double.parseDouble(registro[15].replace(",", ".")));
                    trusted.setMem_perc2(Double.parseDouble(registro[16].replace(",", ".")));

                    // process 3
                    trusted.setNome3(registro[17]);
                    trusted.setCpu_perc3(Double.parseDouble(registro[18].replace(",", ".")));
                    trusted.setMem_perc3(Double.parseDouble(registro[19].replace(",", ".")));

                    // process 4
                    trusted.setNome4(registro[20]);
                    trusted.setCpu_perc4(Double.parseDouble(registro[21].replace(",", ".")));
                    trusted.setMem_perc4(Double.parseDouble(registro[22].replace(",", ".")));

                    // process 5 (exemplo do seu snippet: indices 23,24,25)
                    trusted.setNome5(registro[23]);
                    trusted.setCpu_perc5(Double.parseDouble(registro[24].replace(",", ".")));
                    trusted.setMem_perc5(Double.parseDouble(registro[25].replace(",", ".")));

                    // process 6
                    trusted.setNome6(registro[26]);
                    trusted.setCpu_perc6(Double.parseDouble(registro[27].replace(",", ".")));
                    trusted.setMem_perc6(Double.parseDouble(registro[28].replace(",", ".")));

                    // process 7
                    trusted.setNome7(registro[29]);
                    trusted.setCpu_perc7(Double.parseDouble(registro[30].replace(",", ".")));
                    trusted.setMem_perc7(Double.parseDouble(registro[31].replace(",", ".")));

                    // process 8
                    trusted.setNome8(registro[32]);
                    trusted.setCpu_perc8(Double.parseDouble(registro[33].replace(",", ".")));
                    trusted.setMem_perc8(Double.parseDouble(registro[34].replace(",", ".")));

                    // process 9
                    trusted.setNome9(registro[35]);
                    trusted.setCpu_perc9(Double.parseDouble(registro[36].replace(",", ".")));
                    trusted.setMem_perc9(Double.parseDouble(registro[37].replace(",", ".")));

                    // process 10
//                    trusted.setNome10(registro[38]);
//                    trusted.setCpu_perc10(Double.parseDouble(registro[39].replace(",", ".")));
//                    trusted.setMem_perc10(Double.parseDouble(registro[40].replace(",", ".")));

                    listaTrusted.add(trusted);

                }

            }
            String csvtratado=gerarCsvTrusted(listaTrusted);
            ConexaoAws.enviarCsvClient("dashboardprocesso.csv", csvtratado);

            //List<Map<String, Object>> dadosProcessados = processarDados(dadosMainframe);
            //salvarJsonLocal(dadosProcessados, ARQUIVO_SAIDA_JSON);
        }


    }

    public static String gerarCsvTrusted(List<TrustedCampos> listatrusted) {
        StringBuilder sb = new StringBuilder();

        sb.append("macAdress;timestamp;identificao-mainframe;uso_cpu_total_perc;uso_ram_total_perc;uso_disco_total_perc;disco_throughput_mbs;disco_iops_total;disco_read_count;disco_write_count;disco_latencia_ms;nome1;cpu_perc1;mem_perc1;nome2;cpu_perc2;mem_perc2;nome3;cpu_perc3;mem_perc3;nome4;cpu_perc4;mem_perc4;nome5;cpu_perc5;mem_perc5;nome6;cpu_perc6;mem_perc6;nome7;cpu_perc7;mem_perc7;nome8;cpu_perc8;mem_perc8;nome9;cpu_perc9;mem_perc9;\n");

        for (TrustedCampos trusted : listatrusted) {
            
            sb.append(String.format(Locale.ROOT,
                    "%s;%s;%s;%.2f;%.2f;%.2f;%.2f;%d;%d;%d;%.2f;%s;%.2f;%.2f;%s;%.2f;%.2f;%s;%.2f;%.2f;%s;%.2f;%.2f;%s;%.2f;%.2f;%s;%.2f;%.2f;%s;%.2f;%.2f;%s;%.2f;%.2f;%s;%.2f;%.2f\n",
                    trusted.getMacAdress(),
                    trusted.getTimestamp(),
                    trusted.getIdentificao_mainframe(),
                    trusted.getUso_cpu_total_perc(),
                    trusted.getUso_ram_total_perc(),
                    trusted.getUso_disco_total_perc(),
                    trusted.getDisco_throughput_mbs(),
                    trusted.getDisco_iops_total().intValue(),
                    trusted.getDisco_read_count(),
                    trusted.getDisco_write_count(),
                    trusted.getDisco_latencia_ms(),

                    trusted.getNome1(), trusted.getCpu_perc1(), trusted.getMem_perc1(),
                    trusted.getNome2(), trusted.getCpu_perc2(), trusted.getMem_perc2(),
                    trusted.getNome3(), trusted.getCpu_perc3(), trusted.getMem_perc3(),
                    trusted.getNome4(), trusted.getCpu_perc4(), trusted.getMem_perc4(),
                    trusted.getNome5(), trusted.getCpu_perc5(), trusted.getMem_perc5(),
                    trusted.getNome6(), trusted.getCpu_perc6(), trusted.getMem_perc6(),
                    trusted.getNome7(), trusted.getCpu_perc7(), trusted.getMem_perc7(),
                    trusted.getNome8(), trusted.getCpu_perc8(), trusted.getMem_perc8(),
                    trusted.getNome9(), trusted.getCpu_perc9(), trusted.getMem_perc9()
            ));


        }

        return sb.toString();
    }
}
