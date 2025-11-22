package sptech.school;
import io.github.cdimascio.dotenv.Dotenv;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class DetalheMainframe {

    public static void main(String[] args) {

       List<String[]> linhas = ConexaoAws.lerCsvLocal("trusted.csv");
            // (linha)[coluna]
            // pegar por mainframe - fazer selects nas dashs.
            // fazer select dos mainframes cadastrados.

         Integer ultimo = linhas.size() - 1 ;

         ArrayList<Double> cpu = new ArrayList<>();
         ArrayList<Double> ram = new ArrayList<>();
         ArrayList<Double> disco = new ArrayList<>();

         Long macAdress = Long.valueOf(linhas.get(ultimo)[0].replace(",", "."));
         Double throughput = Double.valueOf(linhas.get(ultimo)[10].replace(",", "."));
         Double iops = Double.valueOf(linhas.get(ultimo)[10].replace(",", "."));
         Double latencia = Double.valueOf(linhas.get(ultimo)[13].replace(",", "."));
         Integer ultimoDado = linhas.size() - 1;

        for (int i = 0; i < 5; i++) {
            cpu.add(Double.valueOf(linhas.get(ultimoDado)[3].replace(",", ".")));//cpu
            ram.add(Double.valueOf(linhas.get(ultimoDado)[4].replace(",", ".")));//ram
            disco.add(Double.valueOf(linhas.get(ultimoDado)[5].replace(",", ".")));//Disco
            ultimoDado--;
        }

        String incidentes = "";
        String minUltimoAlerta = "";
        String hhmm = "";
        String fabricante = "";
        String modelo = "";

        try (Connection conn = DriverManager.getConnection(
                Dotenv.load().get("DB_URL"),
                Dotenv.load().get("DB_USER"),
                Dotenv.load().get("DB_PASSWORD"))) {

            List dadosDb = ConexaoBd.buscarMetricas(conn,macAdress);


            minUltimoAlerta = dadosDb.get(0).toString();
            incidentes = dadosDb.get(1).toString();
            fabricante = dadosDb.get(2).toString();
            modelo = dadosDb.get(3).toString();


            Integer hora = (Integer.valueOf(minUltimoAlerta) / 60);
            Integer minSobra =(Integer.valueOf(minUltimoAlerta) % 60);
            System.out.println("Hora:" + hora + "Min: " + minSobra);
            hhmm = hora +"h "+ minSobra+"m ";
        } catch (SQLException e) {
            System.err.println("Erro ao conectar no banco: " + e.getMessage());
        }

        System.out.println(
                "\n MAC: " + macAdress +
                "\n Fabricante " + fabricante +
                "\n Modelo " + modelo +
                "\n CPU: " + cpu +
                "\n Disco: " + disco +
                "\n through: " + throughput+
                "\n RAM: " + ram +
                "\n iops: " + iops+
                "\n latencia: " + latencia +
                "\n incidentes: " + incidentes +
                "\n tempoUltimoAlerta: " + hhmm );
        }





    }
