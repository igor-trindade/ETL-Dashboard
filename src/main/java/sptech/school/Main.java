package sptech.school;

import io.github.cdimascio.dotenv.Dotenv;
import software.amazon.awssdk.awscore.util.SignerOverrideUtils;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.io.*;
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

         System.out.println( linhas.get(0)[0].toString() + " : "+ linhas.get(ultimo)[0].toString());
         System.out.println( linhas.get(0)[3].toString() + " : "+ linhas.get(ultimo)[3].toString());
         System.out.println( linhas.get(0)[4].toString() + " : "+ linhas.get(ultimo)[4].toString());
         System.out.println( linhas.get(0)[8].toString() + " : "+ linhas.get(ultimo)[8].toString());
         System.out.println( linhas.get(0)[5].toString() + " : "+ linhas.get(ultimo)[5].toString());

         String macAdress = linhas.get(1)[0];




    }

}

