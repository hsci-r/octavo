package parameters
import enumeratum.EnumEntry
import enumeratum.Enum
import play.api.http.MimeTypes

sealed abstract class ResponseFormat extends EnumEntry {
  val mimeType: String
}

case object ResponseFormat extends Enum[ResponseFormat] {
  case object JSON extends ResponseFormat {
    val mimeType = MimeTypes.JSON
  }
  case object CSV extends ResponseFormat {
    val mimeType = MimeTypes.TEXT
  }
  val values = findValues
}
