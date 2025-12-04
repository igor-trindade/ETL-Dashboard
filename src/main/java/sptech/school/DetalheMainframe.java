    package sptech.school;

    import io.github.cdimascio.dotenv.Dotenv;

    import java.lang.invoke.CallSite;
    import java.sql.ClientInfoStatus;
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.SQLException;
    import java.util.*;
    import java.io.FileWriter;
    import java.io.IOException;
    import com.google.gson.Gson;
    import com.google.gson.GsonBuilder;
    import software.amazon.awssdk.services.s3.endpoints.internal.Value;

    public class DetalheMainframe {

        private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

        public static void main(String[] args) {

            List<String> idEmpresas = new ArrayList<>();

            try (Connection conn = DriverManager.getConnection(
                    Dotenv.load().get("DB_URL"),Dotenv.load().get("DB_USER"),Dotenv.load().get("DB_PASSWORD"))) {
                idEmpresas = ConexaoBd.listaEmpresas(conn);
            } catch (SQLException e) {
                System.err.println("Erro ao conectar no banco: " + e.getMessage());
            }

            //  array de empresas
            List<Map<String, Object>> empresasJson = new ArrayList<>();
            for(String empresa : idEmpresas){

                List<Map<String, Object>> mainframesJson = new ArrayList<>();

                List<String> dirs = ConexaoAws.listarDiretorios(empresa);

                for (String dir : dirs) {

                    String mac = dir.replace("1/", "").replace("/", "");

                    List<String[]> linhas = ConexaoAws.lerArquivoCsvDoTrusted(mac, empresa, "trusted.csv");

                    if (linhas.isEmpty()) continue;

                    Integer ultimo = linhas.size() - 2;
                    System.out.println(ultimo);
                    ArrayList<Double> cpu = new ArrayList<>();
                    ArrayList<Double> ram = new ArrayList<>();
                    ArrayList<Double> disk = new ArrayList<>();
                    ArrayList<String> dthr = new ArrayList<>();
                    String macAdress = linhas.get(ultimo)[0].replace(",", ".");

                    System.out.println(linhas.get(ultimo)[1]);
                    Double throughput = Double.valueOf(linhas.get(ultimo)[6].replace(",", "."));
                    Double iops = Double.valueOf(linhas.get(ultimo)[7].replace(",", "."));
                    Double latencia = Double.valueOf(linhas.get(ultimo)[10].replace(",", "."));
                    Integer ultimoDado = linhas.size() - 2;

                    for (int i = 0; i < 5; i++) {
                        cpu.add(Double.valueOf(linhas.get(ultimoDado)[3].replace(",", ".")));
                        ram.add(Double.valueOf(linhas.get(ultimoDado)[4].replace(",", ".")));
                        disk.add(Double.valueOf(linhas.get(ultimoDado)[5].replace(",", ".")));
                        dthr.add(linhas.get(ultimoDado)[1]);
                        ultimoDado--;
                    }

                    String incidentes = "";
                    String minUltimoAlerta = "";
                    String hhmm = "";
                    String fabricante = "";
                    String modelo = "";
                    List<String> metricas = new ArrayList<>();
                    try (Connection conn = DriverManager.getConnection(
                            Dotenv.load().get("DB_URL"),Dotenv.load().get("DB_USER"),Dotenv.load().get("DB_PASSWORD"))) {

                        List dadosDb = ConexaoBd.buscarMetricas(conn, macAdress);
                        metricas = ConexaoBd.buscarMinMax(conn,macAdress);

                        if (!dadosDb.isEmpty()) {
                            minUltimoAlerta = dadosDb.get(0).toString();
                            incidentes = dadosDb.get(1).toString();
                            fabricante = dadosDb.get(2).toString();
                            modelo = dadosDb.get(3).toString();

                            Integer hora = Integer.valueOf(minUltimoAlerta) / 60;
                            Integer minSobra = Integer.valueOf(minUltimoAlerta) % 60;
                            hhmm = hora + "h " + minSobra + "m";
                        }

                    } catch (SQLException e) {
                        System.err.println("Erro ao conectar no banco: " + e.getMessage());
                    }

                    Map<String, Object> mainframe = new HashMap<>();
                    mainframe.put("modelo", modelo);
                    mainframe.put("cpu", cpu);
                    mainframe.put("ram", ram);
                    mainframe.put("disk", disk);
                    mainframe.put("Fabricante", fabricante);
                    mainframe.put("mac", macAdress);
                    mainframe.put("throughput", throughput);
                    mainframe.put("eventos", Integer.valueOf(incidentes));
                    mainframe.put("iops", iops);
                    mainframe.put("latencia", latencia);
                    mainframe.put("metricas",metricas);
                    mainframe.put("tempoAlerta",hhmm);
                    mainframe.put("dt",dthr);

                    mainframesJson.add(mainframe);
                }

                // vinculando mainframe á empresa
                Map<String, Object> empresaJson = new HashMap<>();
                empresaJson.put("empresa", empresa);
                empresaJson.put("mainframes", mainframesJson);

                empresasJson.add(empresaJson);
            }

            // gerar JSON
            String jsonFinal = gson.toJson(empresasJson);

            System.out.println(jsonFinal);

            try (FileWriter writer = new FileWriter("mainframes.json")) {
                writer.write(jsonFinal);
                ConexaoAws.salvarJsonNoS3("detalhesMainframe.json", jsonFinal);
            } catch (IOException e) {
                System.err.println("Erro ao salvar JSON: " + e.getMessage());
            }
        }
        public void executar(){

            List<String> idEmpresas = new ArrayList<>();

            try (Connection conn = DriverManager.getConnection(
                    Dotenv.load().get("DB_URL"),Dotenv.load().get("DB_USER"),Dotenv.load().get("DB_PASSWORD"))) {
                idEmpresas = ConexaoBd.listaEmpresas(conn);
            } catch (SQLException e) {
                System.err.println("Erro ao conectar no banco: " + e.getMessage());
            }

            //  array de empresas
            List<Map<String, Object>> empresasJson = new ArrayList<>();

            for(String empresa : idEmpresas){

                List<Map<String, Object>> mainframesJson = new ArrayList<>();

                List<String> dirs = ConexaoAws.listarDiretorios(empresa);

                for (String dir : dirs) {

                    String mac = dir.replace("1/", "").replace("/", "");

                    List<String[]> linhas = ConexaoAws.lerArquivoCsvDoTrusted(mac, empresa, "trusted.csv");

                    if (linhas.isEmpty()) continue;

                    Integer ultimo = linhas.size() - 1;

                    ArrayList<Double> cpu = new ArrayList<>();
                    ArrayList<Double> ram = new ArrayList<>();
                    ArrayList<Double> disk = new ArrayList<>();
                    ArrayList<String> dthr = new ArrayList<>();
                    String macAdress = linhas.get(ultimo)[0].replace(",", ".");
                    Double throughput = Double.valueOf(linhas.get(ultimo)[6].replace(",", "."));
                    Double iops = Double.valueOf(linhas.get(ultimo)[7].replace(",", "."));
                    Double latencia = Double.valueOf(linhas.get(ultimo)[10].replace(",", "."));
                    Integer ultimoDado = linhas.size() - 1;

                    for (int i = 0; i < 5; i++) {
                        cpu.add(Double.valueOf(linhas.get(ultimoDado)[3].replace(",", ".")));
                        ram.add(Double.valueOf(linhas.get(ultimoDado)[4].replace(",", ".")));
                        disk.add(Double.valueOf(linhas.get(ultimoDado)[5].replace(",", ".")));
                        dthr.add(linhas.get(ultimoDado)[1]);
                        ultimoDado--;
                    }

                    String incidentes = "";
                    String minUltimoAlerta = "";
                    String hhmm = "";
                    String fabricante = "";
                    String modelo = "";
                    List<String> metricas = new ArrayList<>();
                    try (Connection conn = DriverManager.getConnection(
                            Dotenv.load().get("DB_URL"),Dotenv.load().get("DB_USER"),Dotenv.load().get("DB_PASSWORD"))) {

                        List dadosDb = ConexaoBd.buscarMetricas(conn, macAdress);
                        metricas = ConexaoBd.buscarMinMax(conn,macAdress);

                        if (!dadosDb.isEmpty()) {
                            minUltimoAlerta = dadosDb.get(0).toString();
                            incidentes = dadosDb.get(1).toString();
                            fabricante = dadosDb.get(2).toString();
                            modelo = dadosDb.get(3).toString();

                            Integer hora = Integer.valueOf(minUltimoAlerta) / 60;
                            Integer minSobra = Integer.valueOf(minUltimoAlerta) % 60;
                            hhmm = hora + "h " + minSobra + "m";
                        }

                    } catch (SQLException e) {
                        System.err.println("Erro ao conectar no banco: " + e.getMessage());
                    }

                    Map<String, Object> mainframe = new HashMap<>();
                    mainframe.put("modelo", modelo);
                    mainframe.put("cpu", cpu);
                    mainframe.put("ram", ram);
                    mainframe.put("disk", disk);
                    mainframe.put("Fabricante", fabricante);
                    mainframe.put("mac", macAdress);
                    mainframe.put("throughput", throughput);
                    mainframe.put("eventos", Integer.valueOf(incidentes));
                    mainframe.put("iops", iops);
                    mainframe.put("latencia", latencia);
                    mainframe.put("metricas",metricas);
                    mainframe.put("tempoAlerta",hhmm);

                    mainframesJson.add(mainframe);
                }

                // vinculando mainframe á empresa
                Map<String, Object> empresaJson = new HashMap<>();
                empresaJson.put("empresa", empresa);
                empresaJson.put("mainframes", mainframesJson);

                empresasJson.add(empresaJson);
            }

            // gerar JSON
            String jsonFinal = gson.toJson(empresasJson);

            System.out.println(jsonFinal);

            try (FileWriter writer = new FileWriter("mainframes.json")) {
                writer.write(jsonFinal);
                ConexaoAws.salvarJsonNoS3("detalhesMainframe.json", jsonFinal);
            } catch (IOException e) {
                System.err.println("Erro ao salvar JSON: " + e.getMessage());
            }
        }

    }
