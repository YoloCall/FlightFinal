package com.dauphine.flight.conf

object Constant {

  // Local
  val hdfsFlightLocalPath: String = "hdfs://127.0.0.1:9000/data/flight/201306.csv"
  val hdfsWeatherLocalPath: String = "hdfs://127.0.0.1:9000/data/weather/201306hourly.txt"
  val hdfsAirportLocalPath: String = "hdfs://127.0.0.1:9000/data/wban/wban_airport_timezone.csv"
  val hdfsFlightLocalOuput : String = "hdfs://127.0.0.1:9000/data/output/flight"
  val hdfsWeatherLocalOuput : String = "hdfs://127.0.0.1:9000/data/output/weather"
  val hdfsAirportLocalOuput : String = "hdfs://127.0.0.1:9000/data/output/airport"
  val hdfsFlightSilverLocalOuput : String = "hdfs://127.0.0.1:9000/data/output/flightSilver"
  val hdfsGoldLocalOuput : String = "hdfs://127.0.0.1:9000/data/output/gold"
  val hdfsPredicLocalOuput : String = "hdfs://127.0.0.1:9000/data/output/predict"

  // Remote
  val hdfsFlightRemotePath: String = "hdfs://127.0.0.1:9000/data/flight/*.csv"
  val hdfsWeatherRemotePath: String = "hdfs://127.0.0.1:9000/data/weather/*.txt"
  val hdfsAirportRemotePath: String = "hdfs://127.0.0.1:9000/data/wban/wban_airport_timezone.csv"
  val hdfsFlightRemoteOuput : String = "hdfs://127.0.0.1:9000/data/output/flight"
  val hdfsWeatherRemoteOuput : String = "hdfs://127.0.0.1:9000/data/output/weather"
  val hdfsAirportRemoteOuput : String = "hdfs://127.0.0.1:9000/data/output/airport"
  val hdfsFlightSilverRemoteOuput : String = "hdfs://127.0.0.1:9000/data/output/flightSilver"
  val hdfsGoldRemoteOuput : String = "hdfs://127.0.0.1:9000/data/output/gold"
  val hdfsPredicRemoteOuput : String = "hdfs://127.0.0.1:9000/data/output/predict"

  // Dauphine
  val hdfsFlightDauphinePath: String = "hdfs://vmhadoopmaster:9000/students/iasd_20222023/mperez/flightProjet/flight/*.csv"
  val hdfsWeatherDauphinePath: String = "hdfs://vmhadoopmaster:9000/students/iasd_20222023/mperez/flightProjet/weather/*.txt"
  val hdfsAirportDauphinePath: String = "hdfs://vmhadoopmaster:9000/students/iasd_20222023/mperez/flightProjet/wban/wban_airport_timezone.csv"
  val hdfsFlightDauphineOuput : String = "hdfs://vmhadoopmaster:9000/students/iasd_20222023/mperez/flightProjet/output/flight"
  val hdfsWeatherDauphineOuput : String = "hdfs://vmhadoopmaster:9000/students/iasd_20222023/mperez/flightProjet/output/weather"
  val hdfsAirportDauphineOuput : String = "hdfs://vmhadoopmaster:9000/students/iasd_20222023/mperez/flightProjet/output/airport"
  val hdfsFlightSilverDauphineOuput : String = "hdfs://vmhadoopmaster:9000/students/iasd_20222023/mperez/flightProjet/output/flightSilver"
  val hdfsGoldDauphineOuput : String = "hdfs://vmhadoopmaster:9000/students/iasd_20222023/mperez/flightProjet/output/gold"


}
