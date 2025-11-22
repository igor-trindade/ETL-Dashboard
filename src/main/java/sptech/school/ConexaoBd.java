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
    public static List<Object> buscarMetricas(Connection conn, Long macAdress) throws SQLException {
        String sql = """
                SELECT TIMESTAMPDIFF(MINUTE, al.dt_hora, NOW()) AS dif_ultimo_alerta,
                                    (SELECT COUNT(*)
                                        FROM alerta al2
                                        JOIN metrica mt2 ON mt2.id = al2.fkMetrica
                                        JOIN mainframe m2 ON m2.id = mt2.fkMainframe
                                        WHERE m2.macAdress = m.macAdress
                                          AND TIMESTAMPDIFF(HOUR, al2.dt_hora, NOW()) < 24
                                    ) AS incidentes_ultimas_24, m.fabricante, m.modelo
                                FROM mainframe AS m
                                JOIN metrica AS mt ON m.id = mt.fkMainframe
                                JOIN alerta AS al ON mt.id = al.fkMetrica
                                WHERE m.macAdress = ?
                                ORDER BY al.dt_hora DESC
                                LIMIT 1;
        """;

        List lista = new ArrayList<>();

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, macAdress);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                lista.add(rs.getString("dif_ultimo_alerta"));
                lista.add(rs.getString("incidentes_ultimas_24"));
                lista.add(rs.getString("fabricante"));
                lista.add(rs.getString("modelo"));
            }
        }
        return lista;
    }
   // busca todos mainframes




}
