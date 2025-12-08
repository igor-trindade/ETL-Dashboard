package sptech.school;

import java.util.Locale;
public class Alerta {

    private String dtHora;
    private Double valorColetado;
    private String componente;
    private String macAdress;
    private String identificacaoMainframe;

    public Alerta(String dtHora, Double valorColetado, String componente, String macAdress, String identificacaoMainframe) {

        this.dtHora = dtHora;
        this.valorColetado = valorColetado;
        this.componente = componente;
        this.macAdress = macAdress;
        this.identificacaoMainframe = identificacaoMainframe;
    }

    @Override
    public String toString() {
        return String.format(Locale.US,
                "  {\n" +
                        "    \"dtHora\": \"%s\",\n" +
                        "    \"valorColetado\": %.2f,\n" +
                        "    \"componente\": \"%s\",\n" +
                        "    \"macAdress\": \"%s\",\n" +
                        "    \"identificacaoMainframe\": \"%s\"\n" +
                        "  }",
                dtHora, valorColetado, componente, macAdress, identificacaoMainframe
        );
    }
}
