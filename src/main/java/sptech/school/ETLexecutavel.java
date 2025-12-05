package sptech.school;

public class ETLexecutavel {
    public static void main(String[] args) {
        AlertaMainProcessor alerta=new AlertaMainProcessor();
        AlertaMainProcessorAgregado alertaagregado=new AlertaMainProcessorAgregado();
        DashboardDataProcessor dashboardDataProcessor=new DashboardDataProcessor();
        DashboardProcesso dashboardProcesso=new DashboardProcesso();
        DetalheMainframe detalheMainframe=new DetalheMainframe();
        DetalhesArmazenamento detalhesArmazenamento=new DetalhesArmazenamento();

        alerta.executar();
        alertaagregado.executar();
        dashboardDataProcessor.executar();
        detalheMainframe.executar();
        dashboardProcesso.executar();
        detalhesArmazenamento.executar();
    }
}
