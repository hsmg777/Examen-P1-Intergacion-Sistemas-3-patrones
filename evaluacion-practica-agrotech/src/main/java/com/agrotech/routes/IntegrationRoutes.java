package com.agrotech.routes;

import com.agrotech.model.Lectura;
import com.agrotech.service.ServicioAnalitica;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.csv.CsvDataFormat;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class IntegrationRoutes extends RouteBuilder {
  private final String inputDir, outputDir, processedDir, jdbcUrl;
  public IntegrationRoutes(String in, String out, String prc, String jdbc) {
    this.inputDir=in; this.outputDir=out; this.processedDir=prc; this.jdbcUrl=jdbc;
  }
  @Override public void configure() throws Exception {
    from("timer:bootstrap?repeatCount=1")
      .process(e -> {
        try (Connection con = java.sql.DriverManager.getConnection(jdbcUrl);
             Statement st = con.createStatement()) {
          st.execute("""
            CREATE TABLE IF NOT EXISTS lecturas (
              id_sensor TEXT, fecha TEXT, humedad REAL, temperatura REAL
            )
          """);
        }
      }).log("[BOOT] Tabla 'lecturas' lista.");

    CsvDataFormat csv = new CsvDataFormat().setUseMaps(true).setSkipHeaderRecord(true);

    from("file:" + inputDir + "?include=sensores\\.csv&move=" + processedDir + "/${file:onlyname}")
      .routeId("file-transfer")
      .log("[FILE] Recibido: ${header.CamelFileName}")
      .to("file:" + outputDir)
      .unmarshal(csv)
      .process(ex -> {
        @SuppressWarnings("unchecked") List<Map<String,String>> rows = ex.getIn().getBody(List.class);
        var lecturas = rows.stream().map(r -> new Lectura(
          r.get("id_sensor"), r.get("fecha"),
          Double.parseDouble(r.get("humedad")), Double.parseDouble(r.get("temperatura"))
        )).toList();
        ex.getIn().setBody(lecturas);
      })
      .split(body())
        .marshal().json()
        .to("direct:agroAnalyzerIn")
      .end();

    from("direct:agroAnalyzerIn")
      .routeId("agroanalyzer-insert")
      .log("[ANALYZER] Insertando: ${body}")
      .unmarshal().json(Lectura.class)
      .process(e -> {
        Lectura l = e.getIn().getBody(Lectura.class);
        try (var con = java.sql.DriverManager.getConnection(jdbcUrl);
             var ps = con.prepareStatement(
          "INSERT INTO lecturas (id_sensor, fecha, humedad, temperatura) VALUES (?,?,?,?)")) {
          ps.setString(1,l.id_sensor); ps.setString(2,l.fecha);
          ps.setDouble(3,l.humedad); ps.setDouble(4,l.temperatura);
          ps.executeUpdate();
        }
      })
      .setBody(simple("OK"))
      .log("[ANALYZER] Insert OK");

    from("direct:solicitarLectura")
      .routeId("rpc-cliente")
      .setHeader("id_sensor", body())
      .log("[CLIENTE] Solicitando ${header.id_sensor}")
      .to("direct:rpc.obtenerUltimo?timeout=2000")
      .log("[CLIENTE] Respuesta: ${body}");

    from("direct:rpc.obtenerUltimo")
      .routeId("rpc-servidor")
      .log("[SERVIDOR] Para ${header.id_sensor}")
      .bean(new ServicioAnalitica(jdbcUrl), "getUltimoValor");

    from("timer:rpcTest?delay=3000&repeatCount=1")
      .setBody(constant("S001"))
      .to("direct:solicitarLectura")
      .log("[TEST] RPC S001 OK");
  }
}
