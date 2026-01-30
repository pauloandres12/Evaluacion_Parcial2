import cats.effect.*
import doobie.*
import doobie.implicits.*
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import scala.io.Source

object AtaquesDB extends IOApp.Simple {

  // --- 1. CONFIGURACIÓN ---
  val rutaCsv: String = "C:\\Users\\ASUS\\IdeaProjects\\Examen_2p\\src\\main\\resources\\data\\ataques.csv"

  val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "com.mysql.cj.jdbc.Driver",
    url = "jdbc:mysql://localhost:3306/ataques",
    user = "root",
    password = "Pauloan@2005",
    logHandler = None
  )

  // --- 2. MODELO DE DATOS (Case Class) ---
  final case class RegistroAtaque(
                                   id: Int,
                                   tipoAtaque: String,
                                   ipOrigen: String,
                                   ipDestino: String,
                                   severidad: String,
                                   paisOrigen: String,
                                   paisDestino: String,
                                   fechaAtaque: String, // Doobie insertará el String como DATETIME automáticamente
                                   duracionMinutos: Int,
                                   contenidoSensible: Boolean
                                 )

  // --- 3. CONSULTAS SQL ---
  // Creación de la tabla
  private val crearTablaSQL: ConnectionIO[Int] =
    sql"""
        CREATE TABLE IF NOT EXISTS registro_ataques (
            id INT PRIMARY KEY,
            tipo_ataque VARCHAR(100),
            ip_origen VARCHAR(50),
            ip_destino VARCHAR(50),
            severidad VARCHAR(20),
            pais_origen VARCHAR(50),
            pais_destino VARCHAR(50),
            fecha_ataque DATETIME,
            duracion_minutos INT,
            contenido_sensible BOOLEAN
        )
      """.update.run

  // Inserción masiva de datos
  def guardarRegistros(lista: List[RegistroAtaque]): ConnectionIO[Int] = {
    val sentencia =
      """
        INSERT INTO registro_ataques
        (id, tipo_ataque, ip_origen, ip_destino, severidad, pais_origen,
         pais_destino, fecha_ataque, duracion_minutos, contenido_sensible)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    // Doobie mapea automáticamente la List[RegistroAtaque] a los ? de la consulta
    Update[RegistroAtaque](sentencia).updateMany(lista)
  }

  // --- Lectura del archivo ---
  def extraerDatos(): IO[List[RegistroAtaque]] = IO {
    val archivo = Source.fromFile(rutaCsv)("UTF-8")

    try {
      archivo.getLines()
        .drop(1)
        .toList
        .map { fila =>
          val partes = fila.split(",")
          RegistroAtaque(
            id = partes(0).trim.toInt,
            tipoAtaque = partes(1).trim,
            ipOrigen = partes(2).trim,
            ipDestino = partes(3).trim,
            severidad = partes(4).trim,
            paisOrigen = partes(5).trim,
            paisDestino = partes(6).trim,
            fechaAtaque = partes(7).trim,
            duracionMinutos = partes(8).trim.toInt,
            contenidoSensible = partes(9).trim.toBoolean
          )
        }
    } finally {
      archivo.close()
    }
  }

  // --- Estructura del run ---
  override def run: IO[Unit] = {
    // EL "for" ES OBLIGATORIO para usar "<-"
    for {
      _ <- IO.println(s"--- Iniciando programa ---")

      _ <- crearTablaSQL.transact(xa)
      _ <- IO.println("Tabla verificada.")

      datos <- extraerDatos()
      _ <- IO.println(s"Registros leídos: ${datos.length}")

      filas <- guardarRegistros(datos).transact(xa)
      _ <- IO.println(s"Filas insertadas: $filas")

    } yield () // El yield es obligatorio al final del for
  }
}