package parameters

import java.text.{BreakIterator, CharacterIterator, StringCharacterIterator}

import enumeratum.{Enum, EnumEntry}
import services.OffsetSearchType

import scala.collection.Searching

class ExpandingBreakIterator(protected val sub: BreakIterator, protected val expandLeft: Int = 0, protected val expandRight: Int = 0) extends BreakIterator {

  protected var text: String = _
  protected var firstIndex: Int = _
  protected var lastIndex: Int = _
  override def setText(text: String): Unit = {
    this.text = text
    sub.setText(text)
    this.lastIndex = last()
    this.firstIndex = first()
  }

  override def setText(ci: CharacterIterator): Unit = {
    val sb = new StringBuilder()
    var c = ci.first()
    while (c != CharacterIterator.DONE) {
      sb.append(c)
      c = ci.next()
    }
    this.text = sb.toString
    ci.first()
    sub.setText(ci)
  }

  override def getText: CharacterIterator = sub.getText

  override def first(): Int = sub.first
  override def last(): Int = sub.last
  override def current: Int = sub.current
  override def next(): Int = {
    val lastIndex = this.lastIndex
    (0 until expandRight).foreach(_ => sub.next() match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.next()
  }
  override def previous(): Int = {
    val firstIndex = this.firstIndex
    (0 until expandLeft).foreach(_ => sub.previous() match {
      case `firstIndex` => return firstIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.previous()
  }
  override def next(n: Int): Int = {
    sub.next(n)
    val lastIndex = this.lastIndex
    (0 until expandRight).foreach(_ => sub.next()  match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.current
  }
  override def following(offset: Int): Int = {
    if (sub.following(offset) == BreakIterator.DONE) return BreakIterator.DONE
    val lastIndex = this.lastIndex
    (0 until expandRight).foreach(_ => sub.next() match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.current
  }
  override def preceding(offset: Int): Int = {
    val firstIndex = this.firstIndex
    if (sub.preceding(offset) == BreakIterator.DONE) return BreakIterator.DONE
    (0 until expandLeft).foreach(_ => sub.previous() match {
      case `firstIndex` => return firstIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.current
  }
  
}

class ExpandingWordBreakIterator(expandLeft: Int = 0, expandRight: Int = 0) extends ExpandingBreakIterator(BreakIterator.getWordInstance,expandLeft,expandRight) {
  private def nextWord(): Int = {
    var i = sub.next()
    while (i != BreakIterator.DONE && i != lastIndex && !Character.isLetterOrDigit(text.codePointAt(i)))
      i = sub.next()
    i
  }
  override def next(): Int = {
    val lastIndex = this.lastIndex
    (0 until expandRight).foreach(_ => this.nextWord() match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.next()
  }
  private def previousWord(): Int = {
    var i = sub.previous()
    while (i != BreakIterator.DONE && i != firstIndex && !Character.isLetterOrDigit(text.codePointAt(i))) i = sub.previous()
    i
  }
  override def previous(): Int = {
    val firstIndex = this.firstIndex
    (0 until expandLeft).foreach(_ => this.previousWord() match {
      case `firstIndex` => return firstIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.previous()
  }
  override def next(n: Int): Int = {
    val lastIndex = this.lastIndex
    (0 to n).foreach(_ => this.nextWord() match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    (0 until expandRight).foreach(_ => sub.next()  match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.current
  }
  override def following(offset: Int): Int = {
    val lastIndex = this.lastIndex
    var i = sub.following(offset)
    if (expandRight>0) while (i != BreakIterator.DONE && i != lastIndex && !Character.isLetterOrDigit(text.codePointAt(i)))
      i = sub.next()
    i match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    }
    (0 until expandRight - 1).foreach(_ => this.nextWord() match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    if (expandRight>0) sub.next() else sub.current
  }
  override def preceding(offset: Int): Int = {
    val firstIndex = this.firstIndex
    var i = sub.preceding(offset)
    while (i != BreakIterator.DONE && i != firstIndex && !Character.isLetterOrDigit(text.codePointAt(i)))
      i = sub.previous()
    (0 until expandLeft).foreach(_ => this.previousWord() match {
      case `firstIndex` => return firstIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.current
  }
}

class PatternBreakIterator(pattern: String) extends BreakIterator {

  val pbr = pattern.r

  var breakIndices: Array[Int] = _
  var currentB: Int = 0
  
  var text: String = _
  
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

class ParagraphBreakIterator extends PatternBreakIterator("\n\n")
class WordBreakIterator extends PatternBreakIterator("[\\p{Z}\\p{P}\\p{C}]*")
class LineBreakIterator extends PatternBreakIterator("\n")

sealed abstract class ContextLevel extends EnumEntry {
  def apply(expandLeft: Int, expandRight: Int): BreakIterator
  val defaultStartSearchType: OffsetSearchType.Value
  val defaultEndSearchType: OffsetSearchType.Value
}

object ContextLevel extends Enum[ContextLevel] {
  case object CHARACTER extends ContextLevel {
    def apply(expandLeft: Int, expandRight: Int): BreakIterator = if (expandLeft>0 || expandRight>0) new ExpandingBreakIterator(BreakIterator.getCharacterInstance,expandLeft,expandRight) else BreakIterator.getCharacterInstance
    val defaultStartSearchType = OffsetSearchType.PREV
    val defaultEndSearchType = OffsetSearchType.PREV
  }
  case object TOKEN extends ContextLevel {
    def apply(expandLeft: Int, expandRight: Int): BreakIterator =  if (expandLeft>0 || expandRight>0) new ExpandingBreakIterator(BreakIterator.getWordInstance, expandLeft,expandRight) else BreakIterator.getWordInstance
    val defaultStartSearchType = OffsetSearchType.EXACT
    val defaultEndSearchType = OffsetSearchType.PREV
  }
  case object WORD extends ContextLevel {
    def apply(expandLeft: Int, expandRight: Int): BreakIterator =  new ExpandingWordBreakIterator(expandLeft,expandRight)
    val defaultStartSearchType = OffsetSearchType.EXACT
    val defaultEndSearchType = OffsetSearchType.PREV
  }
  case object SENTENCE extends ContextLevel {
    def apply(expandLeft: Int, expandRight: Int): BreakIterator =  if (expandLeft>0 || expandRight>0) new ExpandingBreakIterator(BreakIterator.getSentenceInstance,expandLeft,expandRight) else BreakIterator.getSentenceInstance
    val defaultStartSearchType = OffsetSearchType.EXACT
    val defaultEndSearchType = OffsetSearchType.PREV
  }
  case object LINE extends ContextLevel {
    def apply(expandLeft: Int, expandRight: Int): BreakIterator = if (expandLeft>0 || expandRight>0) new ExpandingBreakIterator(new LineBreakIterator(),expandLeft,expandRight) else new LineBreakIterator()
    val defaultStartSearchType = OffsetSearchType.EXACT
    val defaultEndSearchType = OffsetSearchType.PREV
  }
  case object PARAGRAPH extends ContextLevel {
    def apply(expandLeft: Int, expandRight: Int): BreakIterator =  if (expandLeft>0 || expandRight>0) new ExpandingBreakIterator(new ParagraphBreakIterator(),expandLeft,expandRight) else new ParagraphBreakIterator()
    val defaultStartSearchType = OffsetSearchType.EXACT
    val defaultEndSearchType = OffsetSearchType.PREV
  }
  val values = findValues
}