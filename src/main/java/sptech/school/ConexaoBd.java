package sptech.school;

import io.github.cdimascio.dotenv.Dotenv;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class ConexaoBd {

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.load();

        String url = dotenv.get("DB_URL");
        String user = dotenv.get("DB_USER");
        String password = dotenv.get("DB_PASSWORD");

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Conexão estabelecida com sucesso! \n");
        } catch (SQLException e) {
            System.err.println("Erro na conexão: " + e.getMessage());
        }
    }

    // Busca métricas configuradas para um mainframe
    public static List<List<Object>> buscarMetricas(Connection conn, String macAdress) throws SQLException {
        String sql = """
        SELECT 
            cm.fkComponente, 
            m.min, 
            m.max, 
            c.nome AS nomeComponente,
            nm.nome AS nomeMetrica
        FROM componente_mainframe cm
        JOIN metrica m ON m.fkComponente = cm.fkComponente
        JOIN nome_metrica nm ON nm.id = m.fkNomeMetrica
        JOIN componente c ON c.id = cm.fkComponente
        JOIN mainframe mf ON mf.id = cm.fkMainframe
        WHERE mf.macAdress = ?
        """;

        List<List<Object>> lista = new ArrayList<>();

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, macAdress);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                lista.add(List.of(
                        rs.getInt("fkComponente"),
                        rs.getDouble("min"),
                        rs.getDouble("max"),
                        rs.getString("nomeComponente"),
                        rs.getString("nomeMetrica")
                ));
            }
        }
        return lista;
    }

}
