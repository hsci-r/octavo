package parameters

import java.text.{BreakIterator, CharacterIterator, StringCharacterIterator}

import enumeratum.{Enum, EnumEntry}
import services.OffsetSearchType

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
    val lastIndex = this.lastIndex
    (0 until expandRight).foreach(_ => sub.next()  match {
      case `lastIndex` => return lastIndex
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.next(n)
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

  private def nextWordEnd(): Int = {
    var i = sub.current()
    if (i == BreakIterator.DONE) return BreakIterator.DONE
    while (true) {
      val nextIndex = sub.next()
      if (nextIndex == BreakIterator.DONE) return BreakIterator.DONE
      while (i != nextIndex) {
        if (Character.isLetterOrDigit(text.codePointAt(i)))
          return nextIndex
        i += 1
      }
    }
    BreakIterator.DONE
  }
  def nextWordStart(): Int = {
    var i = sub.next()
    if (i == BreakIterator.DONE) return BreakIterator.DONE
    while (true) {
      val nextIndex = sub.next()
      if (nextIndex == BreakIterator.DONE) return BreakIterator.DONE
      while (i != nextIndex) {
        if (Character.isLetterOrDigit(text.codePointAt(i)))
          return sub.previous()
        i += 1
      }
    }
    BreakIterator.DONE
  }
  override def next(): Int = {
    (0 until expandRight).foreach(_ => this.nextWordEnd() match {
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    nextWordEnd()
  }
  private def previousWordStart(): Int = {
    var i = sub.current()
    if (i == BreakIterator.DONE) return BreakIterator.DONE
    i -= 1
    while (true) {
      val prevIndex = sub.previous()
      if (prevIndex == BreakIterator.DONE) return BreakIterator.DONE
      while (i != prevIndex - 1) {
        if (Character.isLetterOrDigit(text.codePointAt(i)))
          return prevIndex
        i -= 1
      }
    }
    BreakIterator.DONE
  }
  override def previous(): Int = {
    (0 until expandLeft).foreach(_ => this.previousWordStart() match {
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    previousWordStart()
  }
  override def next(n: Int): Int = {
    (0 to n*expandRight).foreach(_ => this.nextWordEnd() match {
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.current
  }
  def followingWordStart(offset: Int): Int = {
    var i = sub.following(offset)
    if (i == BreakIterator.DONE) return i
    while (true) {
      val nextIndex = sub.next()
      if (nextIndex == BreakIterator.DONE) return BreakIterator.DONE
      while (i != nextIndex) {
        if (Character.isLetterOrDigit(text.codePointAt(i)))
          return sub.previous()
        i += 1
      }
    }
    BreakIterator.DONE
  }
  override def following(offset: Int): Int = {
    sub.following(offset)
    (0 until expandRight).foreach(_ => this.nextWordEnd() match {
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.current()
  }
  override def preceding(offset: Int): Int = {
    sub.preceding(offset)
    (0 until expandLeft).foreach(_ => this.previousWordStart() match {
      case BreakIterator.DONE => return BreakIterator.DONE
      case _ =>
    })
    sub.current()
  }
  def precedingWordStart(offset: Int): Int = {
    var i = offset
    if (i == BreakIterator.DONE) return BreakIterator.DONE
    while (true) {
      val prevIndex = sub.preceding(i)
      if (prevIndex == BreakIterator.DONE) return BreakIterator.DONE
      while (i != prevIndex - 1) {
        if (Character.isLetterOrDigit(text.codePointAt(i))) {
          (0 until expandLeft).foreach(_ => this.previousWordStart() match {
            case BreakIterator.DONE => return BreakIterator.DONE
            case _ =>
          })
          return sub.current()
        }
        i -= 1
      }
    }
    BreakIterator.DONE
  }
}

class NoBreakIterator extends BreakIterator {
  var text: String = _
  override def setText(text: String): Unit = {
    atEnd = false
    this.text = text
  }

  override def setText(ci: CharacterIterator): Unit = throw new UnsupportedOperationException

  override def getText: CharacterIterator = new StringCharacterIterator(text)
  var atEnd: Boolean = false
  override def first(): Int = {
    atEnd = false
    0
  }
  override def last(): Int = {
    atEnd = true
    text.length
  }
  override def current: Int = if (atEnd) text.length else 0
  override def next(): Int = if (atEnd) BreakIterator.DONE else {
    atEnd = true
    text.length
  }
  override def previous(): Int = if (!atEnd) BreakIterator.DONE else {
    atEnd = false
    0
  }
  override def next(n: Int): Int = if (atEnd) BreakIterator.DONE else {
    atEnd = true
    if (n>1) BreakIterator.DONE
    else text.length
  }
  override def following(offset: Int): Int = if (offset<text.length) {
    atEnd = true
    text.length
  } else BreakIterator.DONE
  override def preceding(offset: Int): Int = if (offset>0) {
    atEnd = false
    0
  } else BreakIterator.DONE
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
    currentB = breakIndices.search(offset + 1).insertionPoint
    breakIndices(currentB)
  }
  override def preceding(offset: Int): Int = {
    if (offset <= 0) {
      currentB = 0
      return BreakIterator.DONE
    }
    currentB = breakIndices.search(offset).insertionPoint - 1
    breakIndices(currentB)
  }
}

class ParagraphBreakIterator extends PatternBreakIterator("\n\n")
class WordBreakIterator extends PatternBreakIterator("[\\p{Z}\\p{P}\\p{C}]+")
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
  case object DOCUMENT extends ContextLevel {
    def apply(expandLeft: Int, expandRight: Int): BreakIterator = new NoBreakIterator()
    val defaultStartSearchType = OffsetSearchType.EXACT
    val defaultEndSearchType = OffsetSearchType.EXACT
  }
  val values = findValues
}