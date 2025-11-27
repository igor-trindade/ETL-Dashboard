package sptech.school;

public class MetricaInfo {
    private int idMetrica;
    private double min;
    private double max;

    public MetricaInfo(int idMetrica, double min, double max) {
        this.idMetrica = idMetrica;
        this.min = min;
        this.max = max;
    }

    public int getIdMetrica() {
        return idMetrica;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }
}
