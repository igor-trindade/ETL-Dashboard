package sptech.school;

import kong.unirest.ObjectMapper;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.util.*;

public class ConexaoAws {

    private static final Region REGION = Region.US_EAST_1;
    private static final S3Client s3 = S3Client.builder()
            .region(REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    private static final List<String> BUCKETS_RAW = new ArrayList<>();
    private static final List<String> BUCKETS_TRUSTED = new ArrayList<>();
    private static final List<String> BUCKETS_CLIENT = new ArrayList<>();
    public static void main(String[] args) {
        try {
            System.out.println("Conectando à AWS S3...");
            List<String> buckets = pegarBucketsS3();

            for (String b : buckets) {
                String nome = b.toLowerCase();
                if (nome.contains("raw")) BUCKETS_RAW.add(b);
                else if (nome.contains("trusted")) BUCKETS_TRUSTED.add(b);
                else if (nome.contains("client")) BUCKETS_CLIENT.add(b);
            }

            System.out.println("\nBuckets RAW: " + BUCKETS_RAW);
            System.out.println("Buckets TRUSTED: " + BUCKETS_TRUSTED);
            System.out.println("Buckets CLIENT: " + BUCKETS_CLIENT);

        } catch (Exception e) {
            System.err.println("Erro ao conectar ou listar buckets: " + e.getMessage());
        }
    }

    //Lê um Json do bucket RAW e devolve como lista de linhas
    public static List<String[]> lerArquivoCsvDoRaw(String nomeArquivo) {
        ObjectMapper mapper = new ObjectMapper();
        String bucketTrusted = pegarBucket("trusted");

        try {
            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket(bucketTrusted)
                    .key(nomeArquivo)
                    .build();

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(s3.getObject(getReq)))) {



                Usuario user = mapper.readValue(new File("dados.json"), Usuario.class);

            }

            System.out.println("Arquivo lido do Trusted: " + nomeArquivo);

        } catch (Exception e) {
            System.err.println("Erro ao ler arquivo do Trusted: " + e.getMessage());
        }

        return linhas;
    }

    //Envia Json tratado para o bucket Client
    public static void enviarJsonClient(String nomeArquivo, String conteudoJson) {
        String bucketClient = pegarBucket("client");

        try {
            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucketClient)
                    .key(nomeArquivo)
                    .build();

            s3.putObject(putReq, RequestBody.fromString(conteudoJson));
            System.out.println("✅ Json tratado enviado para Client: " + nomeArquivo);

        } catch (Exception e) {
            System.err.println("Erro ao enviar Json para Client: " + e.getMessage());
        }
    }

    //Acha o bucket certo pelo nome (raw/trusted/client)
    private static String pegarBucket(String tipo) {
        ListBucketsResponse response = s3.listBuckets();
        for (Bucket b : response.buckets()) {
            if (b.name().toLowerCase().contains(tipo)) {
                return b.name();
            }
        }
        throw new RuntimeException("Bucket do tipo '" + tipo + "' não encontrado!");
    }

    //Lista todos os buckets da conta
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

    //Testa conexão e categoriza buckets

}
