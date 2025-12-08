package sptech.school;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class DashboardDataProcessor {

    private static final String NOME_ARQUIVO_DADOS = "trusted.csv";
    private static final String ARQUIVO_SAIDA_JSON = "dashboard_data.json";
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final DateTimeFormatter TS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Índices das colunas (segundo seu header)
    private static final Integer INDICE_MAC_ADRESS = 0;
    private static final Integer INDICE_TIMESTAMP = 1;
    private static final Integer INDICE_ID_MAINFRAME = 2;
    private static final Integer INDICE_USO_CPU_TOTAL = 3;
    private static final Integer INDICE_USO_RAM_TOTAL = 4;
    private static final Integer INDICE_USO_DISCO_TOTAL = 5;
    private static final Integer INDICE_DISCO_THROUGHPUT = 6;
    private static final Integer INDICE_DISCO_IOPS = 7;
    private static final Integer INDICE_DISCO_READ_COUNT = 8;
    private static final Integer INDICE_DISCO_WRITE_COUNT = 9;
    private static final Integer INDICE_DISCO_LATENCIA = 10;
    private static final Integer INDICE_PROCESSOS_INICIO = 11;
    private static final Integer NUM_PROCESSOS = 9; // nome1..nome10 conforme header
    private static final Integer DADOS_POR_PROCESSO = 3; // nome, cpu_perc, mem_perc


    private static final Double CPU_MUITO_URGENTE = 85.0;
    private static final Double RAM_MUITO_URGENTE = 90.0;
    private static final Double DISCO_MUITO_URGENTE = 80.0;

    private static final Double CPU_URGENTE = 70.0;
    private static final Double RAM_URGENTE = 80.0;
    private static final Double DISCO_URGENTE = 75.0;

    private static final Double CPU_EMERGENCIA = 95.0;
    private static final Double RAM_EMERGENCIA = 95.0;
    private static final Double DISCO_LATENCIA_EMERGENCIA_MS = 50.0;


    private static final Integer TARGET_ALTA_MIN = 120;
    private static final Integer TARGET_MEDIA_MIN = 240;
    private static final Integer TARGET_BAIXA_MIN = 480;

    public static void main(String[] args) {
        System.out.println("Iniciando DashboardDataProcessorV2...");

        List<String[]> linhas;
        try {
            linhas = lerArquivoCsvLocal(NOME_ARQUIVO_DADOS);
        } catch (IOException e) {
            System.err.println("Erro ao ler arquivo: " + e.getMessage());
            return;
        }

        if (linhas == null || linhas.isEmpty()) {
            System.out.println("Nenhum dado encontrado.");
            return;
        }

        // Conversão para registros estruturados
        List<Record> records = linhas.stream()
                .map(DashboardDataProcessor::parseLinha)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (records.isEmpty()) {
            System.out.println("Nenhum registro válido após parsing.");
            return;
        }

        // Agrupa por período (semana, mes, semestre, ano)
        Map<String, Object> dashboard = new LinkedHashMap<>();
        dashboard.put("semana", gerarPeriodo(records, PeriodFilter.SEMANA));
        dashboard.put("mes", gerarPeriodo(records, PeriodFilter.MES));
        dashboard.put("semestre", gerarPeriodo(records, PeriodFilter.SEMESTRE));
        dashboard.put("ano", gerarPeriodo(records, PeriodFilter.ANO));

        String json = gson.toJson(dashboard);
        salvarJsonLocal(json, ARQUIVO_SAIDA_JSON);

        // Chamada para a integração S3
        try {
            ConexaoAws.salvarJsonNoS3(ARQUIVO_SAIDA_JSON, json);
            System.out.println("JSON enviado para S3 via ConexaoAws.");
        } catch (Exception ex) {
            System.err.println("Falha ao enviar para S3: " + ex.getMessage());
        }

        System.out.println("Processamento finalizado. Arquivo gerado: " + ARQUIVO_SAIDA_JSON);
    }
    private static Map<String, Object> gerarPeriodo(List<Record> allRecords, PeriodFilter periodo) {
        // filtra registros que caem no período
        Instant now = Instant.now();
        List<Record> filtered = allRecords.stream()
                .filter(r -> isInPeriod(r.timestamp, periodo, now))
                .collect(Collectors.toList());

        Map<String, Object> periodoObj = new LinkedHashMap<>();

        // agrupa por mainframe identificacao_mainframe
        Map<String, List<Record>> porMainframe = filtered.stream()
                .collect(Collectors.groupingBy(r -> r.identificacaoMainframe));

        // Para kpi agregado do período
        Integer totalAlertas = 0;
        Integer totalMuitoUrgente = 0;
        Integer totalUrgente = 0;
        Integer totalEmergencia = 0;

        Map<String, Object> mainframesMap = new TreeMap<>(); // ordenado por nome

        // para resumo de componentes
        List<Double> listaCpu = new ArrayList<>();
        List<Double> listaRam = new ArrayList<>();
        List<Double> listaDisco = new ArrayList<>();

        for (Map.Entry<String, List<Record>> e : porMainframe.entrySet()) {
            String mfName = e.getKey();
            List<Record> recs = e.getValue();

            // agregações por mainframe
            Integer registros = recs.size();
            Double avgCpu = recs.stream().mapToDouble(r -> r.usoCpuTotal).average().orElse(0.0);
            Double avgRam = recs.stream().mapToDouble(r -> r.usoRamTotal).average().orElse(0.0);
            Double avgDisco = recs.stream().mapToDouble(r -> r.usoDiscoTotal).average().orElse(0.0);

            // contadores de alertas por tipo
            Integer mturge = 0, urge = 0, emerge = 0;
            Integer cpuAlertCnt = 0, ramAlertCnt = 0, discoAlertCnt = 0;

            // top processos
            Map<String, Double> processCpuSum = new HashMap<>();
            Map<String, Double> processMemSum = new HashMap<>();

            for (Record r : recs) {
                Boolean isEmergencia = (r.usoCpuTotal >= CPU_EMERGENCIA) || (r.usoRamTotal >= RAM_EMERGENCIA) || (r.discoLatenciaMs >= DISCO_LATENCIA_EMERGENCIA_MS);
                Boolean isMuitoUrgente = (r.usoCpuTotal >= CPU_MUITO_URGENTE) || (r.usoRamTotal >= RAM_MUITO_URGENTE) || (r.usoDiscoTotal >= DISCO_MUITO_URGENTE);
                Boolean isUrgente = (r.usoCpuTotal >= CPU_URGENTE) || (r.usoRamTotal >= RAM_URGENTE) || (r.usoDiscoTotal >= DISCO_URGENTE);

                if (isEmergencia) { emerge++; }
                if (isMuitoUrgente) { mturge++; }
                if (isUrgente) { urge++; }

                if (r.usoCpuTotal >= CPU_URGENTE) cpuAlertCnt++;
                if (r.usoRamTotal >= RAM_URGENTE) ramAlertCnt++;
                if (r.usoDiscoTotal >= DISCO_URGENTE || r.discoLatenciaMs >= DISCO_LATENCIA_EMERGENCIA_MS) discoAlertCnt++;

                for (ProcessInfo p : r.processos) {
                    if (p.nome == null || p.nome.trim().isEmpty()) continue;
                    processCpuSum.put(p.nome, processCpuSum.getOrDefault(p.nome, 0.0) + p.cpuPerc);
                    processMemSum.put(p.nome, processMemSum.getOrDefault(p.nome, 0.0) + p.memPerc);
                }
            }

            // top processos cpu
            List<Map<String, Object>> topCpu = processCpuSum.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(5)
                    .map(ent -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("nome", ent.getKey());
                        m.put("cpuPercSum", round(ent.getValue(), 2));
                        return m;
                    }).collect(Collectors.toList());

            // top processos memoria
            List<Map<String, Object>> topMem = processMemSum.entrySet().stream()
                    .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                    .limit(5)
                    .map(ent -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("nome", ent.getKey());
                        m.put("memPercSum", round(ent.getValue(), 2));
                        return m;
                    }).collect(Collectors.toList());

            Integer totalAlertsMf = mturge + urge + emerge;
            totalAlertas += totalAlertsMf;
            totalMuitoUrgente += mturge;
            totalUrgente += urge;
            totalEmergencia += emerge;

            Map<String, Object> tempoSolucaoCalc = calcularTempoSolucaoSimulado(mturge, urge, emerge);

            String componenteCritico = "CPU";
            if (avgRam >= avgCpu && avgRam >= avgDisco) componenteCritico = "RAM";
            else if (avgDisco >= avgCpu && avgDisco >= avgRam) componenteCritico = "DISCO";

            // obj mainframe
            Map<String, Object> mfObj = new LinkedHashMap<>();

            mfObj.put("nome", mfName);
            mfObj.put("registros", registros);
            mfObj.put("avgCpuPerc", round(avgCpu, 2));
            mfObj.put("avgRamPerc", round(avgRam, 2));
            mfObj.put("avgDiscoPerc", round(avgDisco, 2));

            mfObj.put("alertas_total", totalAlertsMf);

            Map<String, Integer> breakdown = new LinkedHashMap<>();
            breakdown.put("mturge", mturge);
            breakdown.put("urge", urge);
            breakdown.put("emerge", emerge);
            mfObj.put("breakdown", breakdown);

            Map<String, Integer> componentesCnt = new LinkedHashMap<>();
            componentesCnt.put("cpu_alerts", cpuAlertCnt);
            componentesCnt.put("ram_alerts", ramAlertCnt);
            componentesCnt.put("disco_alerts", discoAlertCnt);
            mfObj.put("componentes_alerts", componentesCnt);

            mfObj.put("componente_critico", componenteCritico);
            mfObj.put("tempoSolucao", tempoSolucaoCalc);
            mfObj.put("top_processos_cpu", topCpu);
            mfObj.put("top_processos_mem", topMem);

            // sla
            Long saudaveis = recs.stream().filter(r -> r.usoCpuTotal < 95).count();
            Double uptime = registros == 0 ? 0 : (saudaveis * 100.0 / registros);

            Map<String, Object> sla = new LinkedHashMap<>();
            sla.put("contratado", 99.9);
            sla.put("uptime", round(uptime, 2));
            mfObj.put("sla", sla);

            // rto
            Map<String, Object> rto = new LinkedHashMap<>();
            rto.put("estimadoMinutos", 15);
            rto.put("metricaUsada", "tempoMedioRecuperacao");
            mfObj.put("rto", rto);

            mainframesMap.put(mfName, mfObj);

            listaCpu.add(avgCpu);
            listaRam.add(avgRam);
            listaDisco.add(avgDisco);
        }

        Map<String, Object> componentesResumo = gerarResumoComponentes(listaCpu, listaRam, listaDisco);

        // KPI do período
        Map<String, Object> kpis = new LinkedHashMap<>();
        kpis.put("total_alertas", totalAlertas);
        kpis.put("mturge", totalMuitoUrgente);
        kpis.put("urge", totalUrgente);
        kpis.put("emerge", totalEmergencia);

        Double mediaCpu = listaCpu.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        Double mediaRam = listaRam.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        Double mediaDisco = listaDisco.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

        Double disponibilidade = calcularDisponibilidade(mediaCpu, mediaRam, mediaDisco);
        kpis.put("disponibilidade", round(disponibilidade, 2));

        Map<String, Object> tempoSolucaoPeriodo = calcularTempoSolucaoPeriodoAggregate(mainframesMap);

        // rto
        Map<String, Object> rtoPeriodo = new LinkedHashMap<>();
        rtoPeriodo.put("targetMinutes", TARGET_ALTA_MIN);

        int atualRto = (int) tempoSolucaoPeriodo.values().stream()
                .filter(v -> v instanceof Map)
                .map(m -> (Map<?, ?>) m)
                .mapToDouble(m -> {
                    Object valObj = m.get("atualMinutes");
                    return (valObj instanceof Number)
                            ? ((Number) valObj).doubleValue()
                            : 0.0;
                })
                .max()
                .orElse((double) TARGET_ALTA_MIN);

        rtoPeriodo.put("atualMinutes", atualRto);

        periodoObj.put("kpis", kpis);
        periodoObj.put("mainframes", mainframesMap);
        periodoObj.put("componentes", componentesResumo);
        periodoObj.put("tempoSolucao", tempoSolucaoPeriodo);
        periodoObj.put("rto", rtoPeriodo);

        return periodoObj;
    }


    private static Map<String, Object> gerarResumoComponentes(List<Double> listaCpu, List<Double> listaRam, List<Double> listaDisco) {
        Map<String, Object> comp = new LinkedHashMap<>();
        comp.put("cpu", gerarResumoMetric(listaCpu));
        comp.put("ram", gerarResumoMetric(listaRam));
        comp.put("disco", gerarResumoMetric(listaDisco));
        return comp;
    }

    private static Map<String, Object> gerarResumoMetric(List<Double> valores) {
        Map<String, Object> m = new LinkedHashMap<>();
        double avg = valores.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double max = valores.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
        double min = valores.stream().mapToDouble(Double::doubleValue).min().orElse(0.0);
        m.put("avg", round(avg, 2));
        m.put("max", round(max, 2));
        m.put("min", round(min, 2));
        return m;
    }

    private static Map<String, Object> calcularTempoSolucaoSimulado(Integer mturge, Integer urge, Integer emerge) {
        // heurística simples: se tem emergências, atual = target * 1.7 ; muito urgente -> 1.5 ; urgente -> 1.2 ; caso contrário < target
        Map<String, Object> result = new LinkedHashMap<>();

        Double factorAlta = (emerge > 0) ? 1.7 : (mturge > 0 ? 1.5 : (urge > 0 ? 1.2 : 0.9));
        Double factorMedia = (emerge > 0) ? 1.4 : (mturge > 0 ? 1.2 : (urge > 0 ? 1.1 : 0.9));
        Double factorBaixa = (emerge > 0) ? 1.2 : (mturge > 0 ? 1.1 : (urge > 0 ? 1.05 : 0.95));

        Map<String, Object> alta = new LinkedHashMap<>();
        alta.put("targetMinutes", TARGET_ALTA_MIN);
        alta.put("atualMinutes", (int) Math.round(TARGET_ALTA_MIN * factorAlta));

        Map<String, Object> media = new LinkedHashMap<>();
        media.put("targetMinutes", TARGET_MEDIA_MIN);
        media.put("atualMinutes", (int) Math.round(TARGET_MEDIA_MIN * factorMedia));

        Map<String, Object> baixa = new LinkedHashMap<>();
        baixa.put("targetMinutes", TARGET_BAIXA_MIN);
        baixa.put("atualMinutes", (int) Math.round(TARGET_BAIXA_MIN * factorBaixa));

        result.put("alta", alta);
        result.put("media", media);
        result.put("baixa", baixa);
        return result;
    }

    private static Map<String, Object> calcularTempoSolucaoPeriodoAggregate(Map<String, Object> mainframesMap) {

        Integer maxAlta = 0, maxMedia = 0, maxBaixa = 0;

        for (Object v : mainframesMap.values()) {

            if (!(v instanceof Map)) continue;
            Map<?, ?> mf = (Map<?, ?>) v;

            Object tempoObj = mf.get("tempoSolucao");
            if (!(tempoObj instanceof Map)) continue;

            Map<?, ?> tempo = (Map<?, ?>) tempoObj;

            // mt urgente
            Object altaObj = tempo.get("Muito Urgente");
            if (altaObj instanceof Map) {
                Map<?, ?> alta = (Map<?, ?>) altaObj;
                Object valObj = alta.get("atualMinutes");
                Integer val = (valObj instanceof Number) ? ((Number) valObj).intValue() : 0;
                maxAlta = Math.max(maxAlta, val);
            }

            // urgente
            Object mediaObj = tempo.get("Urgente");
            if (mediaObj instanceof Map) {
                Map<?, ?> media = (Map<?, ?>) mediaObj;
                Object valObj = media.get("atualMinutes");
                int val = (valObj instanceof Number) ? ((Number) valObj).intValue() : 0;
                maxMedia = Math.max(maxMedia, val);
            }

            // emergente
            Object baixaObj = tempo.get("Emergente");
            if (baixaObj instanceof Map) {
                Map<?, ?> baixa = (Map<?, ?>) baixaObj;
                Object valObj = baixa.get("atualMinutes");
                int val = (valObj instanceof Number) ? ((Number) valObj).intValue() : 0;
                maxBaixa = Math.max(maxBaixa, val);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();

        Map<String, Object> a = new LinkedHashMap<>();
        a.put("targetMinutes", TARGET_ALTA_MIN);
        a.put("atualMinutes", Math.max(maxAlta, TARGET_ALTA_MIN));

        Map<String, Object> m = new LinkedHashMap<>();
        m.put("targetMinutes", TARGET_MEDIA_MIN);
        m.put("atualMinutes", Math.max(maxMedia, TARGET_MEDIA_MIN));

        Map<String, Object> b = new LinkedHashMap<>();
        b.put("targetMinutes", TARGET_BAIXA_MIN);
        b.put("atualMinutes", Math.max(maxBaixa, TARGET_BAIXA_MIN));

        result.put("alta", a);
        result.put("media", m);
        result.put("baixa", b);

        return result;
    }


    private static double calcularDisponibilidade(Double avgCpu, Double avgRam, Double avgDisco) {
        //  maior uso -> menor disponibilidade
        Double penalty = (avgCpu * 0.05) + (avgRam * 0.03) + (avgDisco * 0.02);
        Double availability = 100.0 - penalty;
        if (availability > 100.0) availability = 100.0;
        if (availability < 0.0) availability = 0.0;
        return availability;
    }

    private static boolean isInPeriod(Instant ts, PeriodFilter periodo, Instant now) {
        Long days = Duration.between(ts, now).toDays();
        switch (periodo) {
            case SEMANA:
                return days <= 7;
            case MES:
                return days <= 30;
            case SEMESTRE:
                return days <= 182;
            case ANO:
                return days <= 365;
            default:
                return false;
        }
    }

    private static double round(Double v, Integer decimals) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return 0.0;
        Double factor = Math.pow(10, decimals);
        return Math.round(v * factor) / factor;
    }

    private static List<String[]> lerArquivoCsvLocal(String caminhoArquivo) throws IOException {
        List<String[]> linhas = new ArrayList<>();
        List<String> raw = Files.readAllLines(Paths.get(caminhoArquivo));
        if (raw.isEmpty()) return linhas;
        Boolean primeira = true;
        for (String l : raw) {
            if (primeira) { primeira = false; continue; } // pula header
            if (l == null || l.trim().isEmpty()) continue;
            linhas.add(l.split(";"));
        }
        return linhas;
    }

    private static Record parseLinha(String[] cols) {
        try {
            if (cols.length < INDICE_PROCESSOS_INICIO + (NUM_PROCESSOS * DADOS_POR_PROCESSO)) ; // ainda permite linhas menores

            String mac = safeGet(cols, INDICE_MAC_ADRESS);
            String tsRaw = safeGet(cols, INDICE_TIMESTAMP);
            String idMainframe = safeGet(cols, INDICE_ID_MAINFRAME);

            // vírgula vira ponto
            Double usoCpu = parseDouble(safeGet(cols, INDICE_USO_CPU_TOTAL));
            Double usoRam = parseDouble(safeGet(cols, INDICE_USO_RAM_TOTAL));
            Double usoDisco = parseDouble(safeGet(cols, INDICE_USO_DISCO_TOTAL));
            Double discoThroughput = parseDouble(safeGet(cols, INDICE_DISCO_THROUGHPUT));
            Double discoIops = parseDouble(safeGet(cols, INDICE_DISCO_IOPS));
            Double discoRead = parseDouble(safeGet(cols, INDICE_DISCO_READ_COUNT));
            Double discoWrite = parseDouble(safeGet(cols, INDICE_DISCO_WRITE_COUNT));
            Double discoLat = parseDouble(safeGet(cols, INDICE_DISCO_LATENCIA));

            // parse timestamp
            LocalDateTime ldt = LocalDateTime.parse(tsRaw.trim(), TS_FORMATTER);
            Instant ts = ldt.atZone(ZoneId.systemDefault()).toInstant();

            // processos
            List<ProcessInfo> processos = new ArrayList<>();
            Integer base = INDICE_PROCESSOS_INICIO;
            for (int i = 0; i < NUM_PROCESSOS; i++) {
                Integer idxNome = base + (i * DADOS_POR_PROCESSO);
                Integer idxCpu = idxNome + 1;
                Integer idxMem = idxNome + 2;
                String nome = safeGet(cols, idxNome);
                String cpuRaw = safeGet(cols, idxCpu);
                String memRaw = safeGet(cols, idxMem);
                if ((nome == null || nome.trim().isEmpty()) && (cpuRaw == null || cpuRaw.trim().isEmpty()) && (memRaw == null || memRaw.trim().isEmpty()))
                    continue;
                Double cpuPerc = parseDoubleAllowNA(cpuRaw);
                Double memPerc = parseDoubleAllowNA(memRaw);
                processos.add(new ProcessInfo(nome == null ? "N/A" : nome, cpuPerc, memPerc));
            }

            return new Record(mac, ts, idMainframe, usoCpu, usoRam, usoDisco, discoThroughput, discoIops, discoRead, discoWrite, discoLat, processos);
        } catch (Exception ex) {
            System.err.println("Erro parse linha: " + ex.getMessage());
            return null;
        }
    }

    private static String safeGet(String[] arr, Integer idx) {
        if (arr == null || idx < 0 || idx >= arr.length) return "";
        return arr[idx] == null ? "" : arr[idx].trim();
    }

    private static double parseDouble(String s) {
        if (s == null) return 0.0;
        String t = s.trim().replace(",", ".").replaceAll("[^0-9.\\-]", "");
        if (t.isEmpty()) return 0.0;
        try { return Double.parseDouble(t); } catch (Exception e) { return 0.0; }
    }

    private static double parseDoubleAllowNA(String s) {
        if (s == null) return 0.0;
        String trimmed = s.trim();
        if (trimmed.equalsIgnoreCase("N/A") || trimmed.equalsIgnoreCase("NA") || trimmed.isEmpty()) return 0.0;
        return parseDouble(trimmed);
    }

    private static void salvarJsonLocal(String json, String arquivo) {
        try (FileWriter fw = new FileWriter(arquivo)) {
            fw.write(json);
            System.out.println("JSON salvo em: " + arquivo);
        } catch (IOException e) {
            System.err.println("Erro salvando JSON: " + e.getMessage());
        }
    }

    private static class Record {
        String macAdress;
        Instant timestamp;
        String identificacaoMainframe;
        Double usoCpuTotal;
        Double usoRamTotal;
        Double usoDiscoTotal;
        Double discoThroughput;
        Double discoIops;
        Double discoReadCount;
        Double discoWriteCount;
        Double discoLatenciaMs;
        List<ProcessInfo> processos;

        public Record(String mac, Instant ts, String idMainframe, Double usoCpu, Double usoRam, Double usoDisco,
                      Double throughput, Double iops, Double read, Double write, Double latencia, List<ProcessInfo> processos) {
            this.macAdress = mac;
            this.timestamp = ts;
            this.identificacaoMainframe = (idMainframe == null || idMainframe.isEmpty()) ? "UNKNOWN" : idMainframe;
            this.usoCpuTotal = usoCpu;
            this.usoRamTotal = usoRam;
            this.usoDiscoTotal = usoDisco;
            this.discoThroughput = throughput;
            this.discoIops = iops;
            this.discoReadCount = read;
            this.discoWriteCount = write;
            this.discoLatenciaMs = latencia;
            this.processos = processos == null ? new ArrayList<>() : processos;
        }
    }

    private static class ProcessInfo {
        String nome;
        Double cpuPerc;
        Double memPerc;

        public ProcessInfo(String nome, Double cpuPerc, Double memPerc) {
            this.nome = nome;
            this.cpuPerc = cpuPerc;
            this.memPerc = memPerc;
        }
    }

    private enum PeriodFilter { SEMANA, MES, SEMESTRE, ANO }


    public void executar(){
        System.out.println("Iniciando DashboardDataProcessorV2...");

        List<String[]> linhas;
        try {
            linhas = lerArquivoCsvLocal(NOME_ARQUIVO_DADOS);
        } catch (IOException e) {
            System.err.println("Erro ao ler arquivo: " + e.getMessage());
            return;
        }

        if (linhas == null || linhas.isEmpty()) {
            System.out.println("Nenhum dado encontrado.");
            return;
        }

        // Conversão para registros estruturados
        List<Record> records = linhas.stream()
                .map(DashboardDataProcessor::parseLinha)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (records.isEmpty()) {
            System.out.println("Nenhum registro válido após parsing.");
            return;
        }

        // Agrupa por período (semana, mes, semestre, ano)
        Map<String, Object> dashboard = new LinkedHashMap<>();
        dashboard.put("semana", gerarPeriodo(records, PeriodFilter.SEMANA));
        dashboard.put("mes", gerarPeriodo(records, PeriodFilter.MES));
        dashboard.put("semestre", gerarPeriodo(records, PeriodFilter.SEMESTRE));
        dashboard.put("ano", gerarPeriodo(records, PeriodFilter.ANO));

        String json = gson.toJson(dashboard);
        salvarJsonLocal(json, ARQUIVO_SAIDA_JSON);

        // Chamada para a integração S3
        try {
            ConexaoAws.salvarJsonNoS3(ARQUIVO_SAIDA_JSON, json);
            System.out.println("JSON enviado para S3 via ConexaoAws.");
        } catch (Exception ex) {
            System.err.println("Falha ao enviar para S3: " + ex.getMessage());
        }

        System.out.println("Processamento finalizado. Arquivo gerado: " + ARQUIVO_SAIDA_JSON);
    }
}
