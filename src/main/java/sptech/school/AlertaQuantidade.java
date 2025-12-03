package sptech.school;

public class AlertaQuantidade {

    private final String macAdress;
    private final String componente;
    private final String gravidade;
    private int qtdAlertas = 0; // Inicializado com 0

    public AlertaQuantidade(String macAdress, String componente, String gravidade) {
        this.macAdress = macAdress;
        this.componente = componente;
        this.gravidade = gravidade;
    }

    public void incrementarQtd() {
        this.qtdAlertas++;
    }

    @Override
    public String toString() {
        return String.format(
                "  {\n" +
                        "    \"macAdress\": \"%s\",\n" +
                        "    \"componente\": \"%s\",\n" +
                        "    \"gravidade\": \"%s\",\n" +
                        "    \"qtdAlertas\": %d\n" +
                        "  }",
                macAdress, componente, gravidade, qtdAlertas
        );
    }
}