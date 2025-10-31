package com.agrotech;

import com.agrotech.routes.IntegrationRoutes;
import org.apache.camel.main.Main;
import java.util.Properties;

public class App {
  public static void main(String[] args) throws Exception {
    Properties p = new Properties();
    p.load(App.class.getClassLoader().getResourceAsStream("application.properties"));
    String in  = p.getProperty("agrotech.input");
    String out = p.getProperty("agrotech.output");
    String prc = p.getProperty("agrotech.processed");
    String jdbc = p.getProperty("agrotech.jdbc.url");

    Main main = new Main();
    main.configure().addRoutesBuilder(new IntegrationRoutes(in, out, prc, jdbc));
    main.run();
  }
}
