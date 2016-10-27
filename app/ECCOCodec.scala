import org.apache.lucene.codecs.FilterCodec
import org.apache.lucene.codecs.lucene62.Lucene62Codec
import org.apache.lucene.codecs.memory.FSTOrdPostingsFormat


class ECCOCodec extends FilterCodec("ECCO",new Lucene62Codec()) {
    override def postingsFormat = new FSTOrdPostingsFormat() 
}