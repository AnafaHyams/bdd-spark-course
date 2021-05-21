package taxi_lab.models

import java.time.LocalDate

case class Trip(driverID: Int, city: String, distance: Int, date: LocalDate)
