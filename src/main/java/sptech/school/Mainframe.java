package sptech.school;

import java.util.ArrayList;
import java.util.List;

public class Mainframe {

    private Integer idMainframe; // Select no banco
    private List<ArrayList> cpu;
    private List<ArrayList> ram;
    private List<ArrayList> disco;
    private String macAdress;
    private Integer eventos;
    private Double throughput;
    private Double iops;
    private Double latencia;


    public Mainframe(List<ArrayList> cpu, List<ArrayList> disco, Integer eventos, Integer idMainframe, Double iops, Double latencia, String macAdress, List<ArrayList> ram, Double throughput) {
        this.cpu = cpu;
        this.disco = disco;
        this.eventos = eventos;
        this.idMainframe = idMainframe;
        this.iops = iops;
        this.latencia = latencia;
        this.macAdress = macAdress;
        this.ram = ram;
        this.throughput = throughput;
    }

}
