package com.agrotech.model;

public class Lectura {
  public String id_sensor;
  public String fecha;
  public double humedad;
  public double temperatura;

  public Lectura() {}
  public Lectura(String id, String f, double h, double t) {
    this.id_sensor = id; this.fecha = f; this.humedad = h; this.temperatura = t;
  }
}
