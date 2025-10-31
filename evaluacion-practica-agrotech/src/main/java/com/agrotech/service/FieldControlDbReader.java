package com.agrotech.service;

import org.apache.camel.Header;
import java.sql.*;

public class FieldControlDbReader {
  private final String jdbcUrl;
  public FieldControlDbReader(String jdbcUrl) { this.jdbcUrl = jdbcUrl; }

  // Lee DIRECTO desde la BD compartida (sin pasar por AgroAnalyzer)
  public String readUltimo(@Header("id_sensor") String id) {
    String sql = """
      SELECT id_sensor, fecha, humedad, temperatura
      FROM lecturas
      WHERE id_sensor = ?
      ORDER BY datetime(fecha) DESC
      LIMIT 1
    """;
    try (Connection con = DriverManager.getConnection(jdbcUrl);
         PreparedStatement ps = con.prepareStatement(sql)) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return String.format(
            "{\"id\":\"%s\",\"humedad\":%.2f,\"temperatura\":%.2f,\"fecha\":\"%s\"}",
            rs.getString("id_sensor"),
            rs.getDouble("humedad"),
            rs.getDouble("temperatura"),
            rs.getString("fecha")
          );
        }
      }
    } catch (Exception e) {
      return "{\"error\":\""+e.getMessage()+"\"}";
    }
    return "{\"id\":\""+id+"\",\"mensaje\":\"Sin datos\"}";
  }
}
