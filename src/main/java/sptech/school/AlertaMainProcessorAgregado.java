package sptech.school;

import io.github.cdimascio.dotenv.Dotenv;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlertaMainProcessorAgregado {

    // configurações do arquivo .env
    private static final Dotenv DOTENV = Dotenv.load();
    private static final String NOME_ARQUIVO_DADOS = "trusted.csv";
    private static final String NOME_ARQUIVO_JSON_SAIDA = "quantidadeAlertas.json";

    public static void main(String[] args) {

    String modoExecucao = DOTENV.get("MODO_EXECUCAO", "SIMULADO");

    // Mapa para acumular a contagem agregada de todos os mainframes
    Map<String, AlertaQuantidade> mapaContagemTotal = new HashMap<>();

        System.out.println("Iniciando Processador de Alertas AGREGADOS em modo: " + modoExecucao.toUpperCase());

        try (Connection conn = ConexaoBd.getConnection()) {

        if (modoExecucao.equalsIgnoreCase("AWS")) {

            // busca lista de todas as empresas no Banco de Dados
            System.out.println("Buscando empresas cadastradas...");
            List<String> listaEmpresas = ConexaoBd.listaEmpresas(conn);

            if (listaEmpresas.isEmpty()) {
                System.out.println("Nenhuma empresa encontrada no banco de dados.");
                return;
            }

            // itera sobre cada empresa encontrada
            for (String idEmpresaStr : listaEmpresas) {
                System.out.println("--------------------------------------------------");
                System.out.println("Processando empresa ID: " + idEmpresaStr);

                System.out.println("Listando diretórios no S3 para a Empresa " + idEmpresaStr + "...");
                List<String> diretoriosMac = ConexaoAws.listarDiretorios(idEmpresaStr);

                if (diretoriosMac.isEmpty()) {
                    System.out.println("Nenhum diretório/mainframe encontrado no S3 para a Empresa " + idEmpresaStr);
                    continue;
                }

                // itera sobre cada diretório (MAC) encontrado no bucket desta empresa
                for (String dir : diretoriosMac) {

                    String prefixo = idEmpresaStr + "/";
                    String mac = dir.substring(prefixo.length()).replace("/", "");

                    System.out.println("Processando MAC: " + mac);

                    // 1. TENTA LER O ARQUIVO GERAL (TRUSTED CONSOLIDADO)
                    List<String[]> dadosParaProcessar = ConexaoAws.lerArquivoGeralCsvDoTrusted(idEmpresaStr, mac);

                    if (!dadosParaProcessar.isEmpty()) {
                        System.out.println(">> Fonte de dados: ARQUIVO GERAL (consolidado)");
                    } else {
                        // 2. FALHA AO LER O GERAL, TENTA LER O ARQUIVO DO DIA ATUAL
                        dadosParaProcessar = ConexaoAws.lerArquivoCsvDoTrustedDiario(idEmpresaStr, mac, NOME_ARQUIVO_DADOS);

                        if (!dadosParaProcessar.isEmpty()) {
                            System.out.println(">> Fonte de dados: ARQUIVO DIÁRIO (hoje)");
                        } else {
                            System.out.println(">> Nenhuma fonte de dados encontrada (Geral ou Diária) para o MAC: " + mac);
                        }
                    }

                    // só processa se houver dados
                    if (!dadosParaProcessar.isEmpty()) {
                        // CHAMA A FUNÇÃO DA SUA NOVA CLASSE DE PROCESSAMENTO AGREGADO
                        Map<String, AlertaQuantidade> contagemMainframe =
                                GeradorAlertasAgregados.processarDadosParaContagem(conn, dadosParaProcessar);

                        // Junta os resultados no mapa total
                        mapaContagemTotal.putAll(contagemMainframe);
                    }
                }
            }

        } else if (modoExecucao.equalsIgnoreCase("SIMULADO")) {
            System.out.println("Modo SIMULADO: Lendo arquivo local (" + NOME_ARQUIVO_DADOS + ")...");
            List<String[]> dadosMainframe = GeradorAlertas.lerArquivoCsvLocal(NOME_ARQUIVO_DADOS);

            // CHAMA A FUNÇÃO DA SUA NOVA CLASSE DE PROCESSAMENTO AGREGADO
            Map<String, AlertaQuantidade> contagemMainframe =
                    GeradorAlertasAgregados.processarDadosParaContagem(conn, dadosMainframe);

            mapaContagemTotal.putAll(contagemMainframe);
        }

        // geração e envio do json de alertas
        if (!mapaContagemTotal.isEmpty()) {

            // CHAMA A FUNÇÃO DA SUA NOVA CLASSE PARA MONTAR O JSON AGREGADO
            String jsonAlertas = GeradorAlertasAgregados.montarJsonAlertasAgregados(mapaContagemTotal);

            System.out.println("--------------------------------------------------");
            System.out.println("Total de linhas agregadas no JSON: " + mapaContagemTotal.size());

            if (modoExecucao.equalsIgnoreCase("AWS")) {
                // Envia para o S3
                ConexaoAws.salvarJsonNoS3(NOME_ARQUIVO_JSON_SAIDA, jsonAlertas);
            } else {
                salvarJsonLocal(NOME_ARQUIVO_JSON_SAIDA, jsonAlertas);
            }
        } else {
            System.out.println("Processamento finalizado. Nenhum alerta foi gerado.");
        }

    } catch (SQLException e) {
        System.err.println(" Erro de conexão/SQL: " + e.getMessage());
        e.printStackTrace();
    } catch (IOException e) {
        System.err.println(" Erro de I/O na leitura local: " + e.getMessage());
    }
}

    public void executar(){

        String modoExecucao = DOTENV.get("MODO_EXECUCAO", "SIMULADO");

        // Mapa para acumular a contagem agregada de todos os mainframes
        Map<String, AlertaQuantidade> mapaContagemTotal = new HashMap<>();

        System.out.println("Iniciando Processador de Alertas AGREGADOS em modo: " + modoExecucao.toUpperCase());

        try (Connection conn = ConexaoBd.getConnection()) {

            if (modoExecucao.equalsIgnoreCase("AWS")) {

                // busca lista de todas as empresas no Banco de Dados
                System.out.println("Buscando empresas cadastradas...");
                List<String> listaEmpresas = ConexaoBd.listaEmpresas(conn);

                if (listaEmpresas.isEmpty()) {
                    System.out.println("Nenhuma empresa encontrada no banco de dados.");
                    return;
                }

                // itera sobre cada empresa encontrada
                for (String idEmpresaStr : listaEmpresas) {
                    System.out.println("--------------------------------------------------");
                    System.out.println("Processando empresa ID: " + idEmpresaStr);

                    System.out.println("Listando diretórios no S3 para a Empresa " + idEmpresaStr + "...");
                    List<String> diretoriosMac = ConexaoAws.listarDiretorios(idEmpresaStr);

                    if (diretoriosMac.isEmpty()) {
                        System.out.println("Nenhum diretório/mainframe encontrado no S3 para a Empresa " + idEmpresaStr);
                        continue;
                    }

                    // itera sobre cada diretório (MAC) encontrado no bucket desta empresa
                    for (String dir : diretoriosMac) {

                        String prefixo = idEmpresaStr + "/";
                        String mac = dir.substring(prefixo.length()).replace("/", "");

                        System.out.println("Processando MAC: " + mac);

                        // 1. TENTA LER O ARQUIVO GERAL (TRUSTED CONSOLIDADO)
                        List<String[]> dadosParaProcessar = ConexaoAws.lerArquivoGeralCsvDoTrusted(idEmpresaStr, mac);

                        if (!dadosParaProcessar.isEmpty()) {
                            System.out.println(">> Fonte de dados: ARQUIVO GERAL (consolidado)");
                        } else {
                            // 2. FALHA AO LER O GERAL, TENTA LER O ARQUIVO DO DIA ATUAL
                            dadosParaProcessar = ConexaoAws.lerArquivoCsvDoTrustedDiario(idEmpresaStr, mac, NOME_ARQUIVO_DADOS);

                            if (!dadosParaProcessar.isEmpty()) {
                                System.out.println(">> Fonte de dados: ARQUIVO DIÁRIO (hoje)");
                            } else {
                                System.out.println(">> Nenhuma fonte de dados encontrada (Geral ou Diária) para o MAC: " + mac);
                            }
                        }

                        // só processa se houver dados
                        if (!dadosParaProcessar.isEmpty()) {
                            // CHAMA A FUNÇÃO DA SUA NOVA CLASSE DE PROCESSAMENTO AGREGADO
                            Map<String, AlertaQuantidade> contagemMainframe =
                                    GeradorAlertasAgregados.processarDadosParaContagem(conn, dadosParaProcessar);

                            // Junta os resultados no mapa total
                            mapaContagemTotal.putAll(contagemMainframe);
                        }
                    }
                }

            } else if (modoExecucao.equalsIgnoreCase("SIMULADO")) {
                System.out.println("Modo SIMULADO: Lendo arquivo local (" + NOME_ARQUIVO_DADOS + ")...");
                List<String[]> dadosMainframe = GeradorAlertas.lerArquivoCsvLocal(NOME_ARQUIVO_DADOS);

                // CHAMA A FUNÇÃO DA SUA NOVA CLASSE DE PROCESSAMENTO AGREGADO
                Map<String, AlertaQuantidade> contagemMainframe =
                        GeradorAlertasAgregados.processarDadosParaContagem(conn, dadosMainframe);

                mapaContagemTotal.putAll(contagemMainframe);
            }

            // geração e envio do json de alertas
            if (!mapaContagemTotal.isEmpty()) {

                // CHAMA A FUNÇÃO DA SUA NOVA CLASSE PARA MONTAR O JSON AGREGADO
                String jsonAlertas = GeradorAlertasAgregados.montarJsonAlertasAgregados(mapaContagemTotal);

                System.out.println("--------------------------------------------------");
                System.out.println("Total de linhas agregadas no JSON: " + mapaContagemTotal.size());

                if (modoExecucao.equalsIgnoreCase("AWS")) {
                    // Envia para o S3
                    ConexaoAws.salvarJsonNoS3(NOME_ARQUIVO_JSON_SAIDA, jsonAlertas);
                } else {
                    salvarJsonLocal(NOME_ARQUIVO_JSON_SAIDA, jsonAlertas);
                }
            } else {
                System.out.println("Processamento finalizado. Nenhum alerta foi gerado.");
            }

        } catch (SQLException e) {
            System.err.println(" Erro de conexão/SQL: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println(" Erro de I/O na leitura local: " + e.getMessage());
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