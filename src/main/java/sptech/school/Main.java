package sptech.school;

import io.github.cdimascio.dotenv.Dotenv;
import software.amazon.awssdk.awscore.util.SignerOverrideUtils;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    public static void main(String[] args) {

       List<String[]> linhas = ConexaoAws.lerCsvLocal("trusted.csv");
            // (linha)[coluna]

         Integer ultimo = linhas.size() - 1 ;

         ArrayList<Double> cpu = new ArrayList<>();
         ArrayList<Double> ram = new ArrayList<>();
         ArrayList<Double> disco = new ArrayList<>();

         Long macAdress = Long.valueOf(linhas.get(ultimo)[0]);
         Double throughput = Double.valueOf(linhas.get(ultimo)[10].replace(",", "."));
         Double iops = Double.valueOf(linhas.get(ultimo)[10].replace(",", "."));
         Double latencia = Double.valueOf(linhas.get(ultimo)[13].replace(",", "."));
        System.out.println( linhas.get(0)[13].toString() + " : "+ linhas.get(ultimo)[13].toString());


        for (int i = 0; i < 5; i++) {
            Integer ultimoDado = linhas.size() - 1;
            cpu.add(Double.valueOf(linhas.get(ultimoDado)[3]));//cpu
            ram.add(Double.valueOf(linhas.get(ultimoDado)[4]));//ram
            disco.add(Double.valueOf(linhas.get(ultimoDado)[8]));//Disco
        }

        Integer eventos;
        }


    }



