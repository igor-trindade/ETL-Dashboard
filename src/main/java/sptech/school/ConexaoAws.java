package sptech.school;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ConexaoAws {

    private static final Region REGION = Region.US_EAST_1;

    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    // LER CSV DO TRUSTED

    public static List<String[]> lerArquivoCsvDoTrusted(String nomeArquivo) {

        List<String[]> linhas = new ArrayList<>();

        String bucket = pegarBucket("trusted");

        try {
            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(nomeArquivo)
                    .build();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(s3.getObject(getReq)))) {

                String linha;
                while ((linha = reader.readLine()) != null) {
                    linhas.add(linha.split(";"));
                }


            }

            System.out.println("Arquivo lido do TRUSTED: " + nomeArquivo);

        } catch (Exception e) {
            System.err.println("Erro ao ler arquivo do TRUSTED: " + e.getMessage());
        }

        return linhas;
    }
    public static List<String[]> lerCsvLocal(String nomeArquivo) {

            List<String[]> linhas = new ArrayList<>();

            try (BufferedReader reader = new BufferedReader(
                    new FileReader(nomeArquivo))) {

                String linha;

                while ((linha = reader.readLine()) != null) {
                    linhas.add(linha.split(";")); // separa do mesmo jeito
                }

                System.out.println("Arquivo lido LOCALMENTE: " + nomeArquivo);

            } catch (Exception e) {
                System.err.println("Erro ao ler arquivo local: " + e.getMessage());
            }

            return linhas;
   }

    // ENVIAR JSON PARA TRUSTED
    public static void enviarJsonTrusted(String nomeArquivo, String json) {

        String bucketTrusted = pegarBucket("trusted");

        try {
            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucketTrusted)
                    .key(nomeArquivo)
                    .contentType("application/json")
                    .build();

            s3.putObject(putReq, RequestBody.fromString(json));
            System.out.println("JSON enviado ao TRUSTED: " + nomeArquivo);

        } catch (Exception e) {
            System.err.println("Erro ao enviar JSON: " + e.getMessage());
        }
    }



    // LISTAR BUCKETS
    public static List<String> pegarBucketsS3() {

        List<String> buckets = new ArrayList<>();

        try {
            ListBucketsResponse response = s3.listBuckets();
            for (Bucket b : response.buckets()) {
                buckets.add(b.name());
            }

        } catch (S3Exception e) {
            System.err.println("Erro ao listar buckets: " + e.awsErrorDetails().errorMessage());
        }

        return buckets;
    }
    // ACHAR BUCKET PELO NOME
    public static String pegarBucket(String tipo) {

        ListBucketsResponse response = s3.listBuckets();
        for (Bucket b : response.buckets()) {
            if (b.name().toLowerCase().contains(tipo.toLowerCase())) {
                return b.name();
            }
        }

        throw new RuntimeException("Bucket do tipo '" + tipo + "' não encontrado!");
    }
}
