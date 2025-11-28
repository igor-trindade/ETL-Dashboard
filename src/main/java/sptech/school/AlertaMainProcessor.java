package sptech.school;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class AlertaMainProcessor {

    // configurações do arquivo .env
    private static final Dotenv DOTENV = Dotenv.load();
    private static final Integer ID_EMPRESA = Integer.valueOf(DOTENV.get("ID_EMPRESA", "1"));
    private static final String NOME_ARQUIVO_DADOS = "trusted.csv";

    public static void main(String[] args) {

        String modoExecucao = DOTENV.get("MODO_EXECUCAO", "SIMULADO");
        List<Alerta> listaAlertas = new ArrayList<>();
        System.out.println("Iniciando Processador de Alertas em modo: " + modoExecucao.toUpperCase());

        try (Connection conn = ConexaoBd.getConnection()) {

            if (modoExecucao.equalsIgnoreCase("AWS")) {

                String idEmpresaStr = String.valueOf(ID_EMPRESA);
                System.out.println("Modo AWS: Listando diretórios no S3 para a Empresa " + idEmpresaStr + "...");
                List<String> diretoriosMac = ConexaoAws.listarDiretorios(idEmpresaStr); // busca diretórios no S3


                if (diretoriosMac.isEmpty()) {
                    System.out.println("Nenhum diretório/mainframe encontrado no S3 para a Empresa " + ID_EMPRESA);
                    return;
                }


                // itera sobre cada diretório encontrado no bucket
                for (String dir : diretoriosMac) {

                    String mac = dir.replace(idEmpresaStr + "/", "").replace("/", "");
                    System.out.println("Processando MAC encontrado no S3: " + mac);
                    List<String[]> dadosAtuais = ConexaoAws.lerArquivoCsvDoTrusted(mac, idEmpresaStr, NOME_ARQUIVO_DADOS); // lê o CSV do dia atual para esse MAC

                    // só processa se houver dados
                    if (dadosAtuais != null && !dadosAtuais.isEmpty()) {
                        GeradorAlertas.processarDadosParaAlertas(conn, dadosAtuais, listaAlertas);
                    } else {
                        System.out.println("Nenhum dado encontrado hoje para o MAC: " + mac);
                    }
                }



            } else if (modoExecucao.equalsIgnoreCase("SIMULADO")) {
                System.out.println("Modo SIMULADO: Lendo arquivo local (" + NOME_ARQUIVO_DADOS + ")...");
                List<String[]> dadosMainframe = GeradorAlertas.lerArquivoCsvLocal(NOME_ARQUIVO_DADOS);
                GeradorAlertas.processarDadosParaAlertas(conn, dadosMainframe, listaAlertas);
            }



            // geração e envio do JSON de Alertas
            if (!listaAlertas.isEmpty()) {
                String jsonAlertas = GeradorAlertas.montarJsonAlertas(listaAlertas);
                String nomeArquivoAlertas = "alertas.json";

                if (modoExecucao.equalsIgnoreCase("AWS")) {
                    // envia pro client
                    ConexaoAws.salvarJsonNoS3(nomeArquivoAlertas, jsonAlertas);
                } else {
                    salvarJsonLocal(nomeArquivoAlertas, jsonAlertas);
                }
            } else {
                System.out.println("Processamento finalizado. Nenhum alerta foi gerado.");
            }


        } catch (SQLException e) {
            System.err.println("Erro de conexão/SQL: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Erro de I/O na leitura local: " + e.getMessage());
        }
    }



    private static void salvarJsonLocal(String nomeArquivo, String jsonContent) {
        try (FileWriter writer = new FileWriter(nomeArquivo)) {
            writer.write(jsonContent);
            System.out.println("JSON de Alertas salvo localmente: " + nomeArquivo);
        } catch (IOException e) {
            System.err.println("Erro ao salvar JSON localmente: " + e.getMessage());
        }
    }
}