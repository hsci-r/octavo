import org.junit.Assert._
import org.junit.Test
import parameters.{ContextLevel, ExpandingWordBreakIterator}

class TestExpandingBreakIterators {

    @Test
    def testExpandingBreakIterator {
        var str = "0 2.!.6 8 A C E"
        var bi = ContextLevel.CHARACTER(0,0)
        bi.setText(str)
        assertEquals("!",str.substring(bi.following(3),bi.next()))
        bi = ContextLevel.CHARACTER(2,2)
        bi.setText(str)
        assertEquals("6 8",str.substring(bi.following(3),bi.next()))
        bi = ContextLevel.WORD(0,0)
        bi.setText(str)
        assertEquals(".!.6",str.substring(3,bi.following(3)))
        assertEquals("2",str.substring(bi.preceding(3),bi.next()))
        str = "@theresa_may @theresa_may, today anything you say is just to distract from the overwhelming success of the #Brexit… https://t.co/0rr5e2WMRp"
        bi = ContextLevel.WORD(0,0)
        bi.setText(str)
        assertEquals("theresa_may",str.substring(bi.preceding(16),bi.following(16)))
        bi = ContextLevel.WORD(2,2)
        bi.setText(str)
        assertEquals("@theresa_may @theresa_may, today anything",str.substring(Math.max(bi.preceding(16),0),bi.following(16)))
        bi = ContextLevel.WORD(1,1)
        bi.setText(str)
        assertEquals("theresa_may @theresa_may, today",str.substring(bi.preceding(16),bi.following(16)))
    }

    @Test
    def testExpandingWordBreakIterator: Unit = {
        var str = "@theresa_may @theresa_may, today anything you say is just to distract from the overwhelming success of the #Brexit… https://t.co/0rr5e2WMRp"
        var bi = new ExpandingWordBreakIterator(0,0)
        bi.setText(str)
        assertEquals("theresa_may",str.substring(bi.preceding(16),bi.following(16)))
        bi = new ExpandingWordBreakIterator(2,2)
        bi.setText(str)
        assertEquals("@theresa_may @theresa_may, today anything",str.substring(Math.max(bi.preceding(16),0),bi.following(16)))
        bi = new ExpandingWordBreakIterator(1,1)
        bi.setText(str)
        assertEquals("theresa_may @theresa_may, today",str.substring(bi.preceding(16),bi.following(16)))
        bi = new ExpandingWordBreakIterator(2,0)
        bi.setText(str)
        assertEquals("@theresa_may @theresa_may",str.substring(Math.max(bi.preceding(16),0),bi.following(16)))
        bi = new ExpandingWordBreakIterator(0,2)
        bi.setText(str)
        assertEquals("theresa_may, today anything",str.substring(Math.max(bi.preceding(16),0),bi.following(16)))
        assertEquals("today anything you",str.substring(bi.followingWordStart(16),bi.next()))
        bi = new ExpandingWordBreakIterator(0,0)
        bi.setText(str)
        assertEquals("today",str.substring(bi.followingWordStart(16),bi.next()))
        bi.followingWordStart(16)
        assertEquals("anything",str.substring(bi.nextWordStart(),bi.next()))
        bi.followingWordStart(16)
        bi.nextWordStart()
        assertEquals("you",str.substring(bi.nextWordStart(),bi.next()))
        assertEquals("anything",str.substring(bi.preceding(38),bi.next()))
        bi.preceding(38)
        assertEquals("today",str.substring(bi.previous(),bi.next()))
        bi.preceding(38)
        bi.previous()
        assertEquals("theresa_may",str.substring(bi.previous(),bi.next()))
    }

}