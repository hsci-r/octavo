package parameters

import java.text.BreakIterator
import enumeratum.EnumEntry
import enumeratum.Enum
import scala.collection.Searching
import java.text.CharacterIterator
import java.text.StringCharacterIterator

class ExpandingBreakIterator(sub: BreakIterator, expand: Int = 0) extends BreakIterator {
  
  override def setText(text: String): Unit = sub.setText(text)
  
  override def setText(ci: CharacterIterator): Unit = sub.setText(ci)
  
  override def getText: CharacterIterator = sub.getText
  
  override def first(): Int = sub.first
  override def last(): Int = sub.last
  override def current: Int = sub.current
  override def next(): Int = {
    (0 until expand).foreach(_ => sub.next())
    sub.next()
  }
  override def previous(): Int = {
    (0 until expand).foreach(_ => sub.previous())
    sub.previous()
  }
  override def next(n: Int): Int = {
    sub.next(n)
    (0 until expand).foreach(_ => if (sub.next() == BreakIterator.DONE) return BreakIterator.DONE) 
    sub.current
  }
  override def following(offset: Int): Int = {
    if (sub.following(offset) == BreakIterator.DONE) return BreakIterator.DONE
    (0 until expand).foreach(_ => if (sub.next() == BreakIterator.DONE) return BreakIterator.DONE)
    sub.current
  }
  override def preceding(offset: Int): Int = {
    if (sub.preceding(offset) == BreakIterator.DONE) return BreakIterator.DONE
    (0 until expand).foreach(_ => if (sub.previous() == BreakIterator.DONE) return BreakIterator.DONE)
    sub.current
  }
  
}

class ParagraphBreakIterator extends BreakIterator {
  
  var breakIndices: Array[Int] = _
  var currentB: Int = 0
  
  var text: String = _
  
  val pbr = "\n\n".r
  
  override def setText(text: String): Unit = {
    this.text = text
    breakIndices = (Seq(0) ++ pbr.findAllMatchIn(text).map(_.end) ++ Seq(text.length)).toArray
    currentB = 0
  }
  
  override def setText(ci: CharacterIterator): Unit = throw new UnsupportedOperationException
  
  override def getText: CharacterIterator = {
    val ret = new StringCharacterIterator(text)
    ret.setIndex(breakIndices(currentB))
    ret
  }
  
  override def first(): Int = { currentB = 0; breakIndices(currentB) }
  override def last(): Int = { currentB = breakIndices.length - 1; breakIndices(currentB) }
  override def current: Int = breakIndices(currentB)
  override def next(): Int = {
    if (currentB == breakIndices.length - 1) return BreakIterator.DONE
    currentB += 1
    breakIndices(currentB)
  }
  override def previous(): Int = {
    if (currentB == 0) return BreakIterator.DONE
    currentB -= 1
    breakIndices(currentB)
  }
  override def next(n: Int): Int = {
    if (currentB + n >= breakIndices.length - 1) {
      currentB = breakIndices.length - 1
      return BreakIterator.DONE
    }
    currentB += n
    breakIndices(currentB)
  }
  override def following(offset: Int): Int = {
    if (offset >= breakIndices.last) {
      currentB = breakIndices.length - 1
      return BreakIterator.DONE
    }
    currentB = Searching.search(breakIndices).search(offset + 1).insertionPoint
    breakIndices(currentB)
  }
  override def preceding(offset: Int): Int = {
    if (offset <= 0) {
      currentB = 0
      return BreakIterator.DONE
    }
    currentB = Searching.search(breakIndices).search(offset).insertionPoint - 1
    breakIndices(currentB)
  }
}

sealed abstract class ContextLevel extends EnumEntry {
  def apply(expand: Int): BreakIterator
}

object ContextLevel extends Enum[ContextLevel] {
  case object CHARACTER extends ContextLevel {
    def apply(expand: Int): BreakIterator = if (expand>0) new ExpandingBreakIterator(BreakIterator.getCharacterInstance,expand) else BreakIterator.getCharacterInstance
  }
  case object WORD extends ContextLevel {
    def apply(expand: Int): BreakIterator =  if (expand>0) new ExpandingBreakIterator(BreakIterator.getWordInstance,expand*2) else BreakIterator.getWordInstance
  }
  case object SENTENCE extends ContextLevel {
    def apply(expand: Int): BreakIterator =  if (expand>0) new ExpandingBreakIterator(BreakIterator.getSentenceInstance,expand) else BreakIterator.getSentenceInstance
  }
  case object PARAGRAPH extends ContextLevel {
    def apply(expand: Int): BreakIterator =  if (expand>0) new ExpandingBreakIterator(new ParagraphBreakIterator(),expand) else new ParagraphBreakIterator()
  }
  val values = findValues
}