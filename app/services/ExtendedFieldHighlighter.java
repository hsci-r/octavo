package services;

import org.apache.lucene.search.uhighlight.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

public class ExtendedFieldHighlighter extends FieldHighlighter {

    public ExtendedFieldHighlighter(String field, FieldOffsetStrategy fieldOffsetStrategy, BreakIterator breakIterator,
                            PassageScorer passageScorer, int maxPassages, int maxNoHighlightPassages,
                            PassageFormatter passageFormatter) {
        super(field, fieldOffsetStrategy, breakIterator, passageScorer, maxPassages, maxNoHighlightPassages, passageFormatter);
    }

    protected Passage[] highlightOffsetsEnums(List<OffsetsEnum> offsetsEnums)
            throws IOException {
        PassageScorer scorer = passageScorer;
        BreakIterator breakIterator = this.breakIterator;
        final int contentLength = breakIterator.getText().getEndIndex();

        PriorityQueue<OffsetsEnum> offsetsEnumQueue = new PriorityQueue<>(offsetsEnums.size() + 1);
        for (OffsetsEnum off : offsetsEnums) {
            off.setWeight(scorer.weight(contentLength, off.freq()));
            off.nextPosition(); // go to first position
            offsetsEnumQueue.add(off);
        }
        offsetsEnumQueue.add(new OffsetsEnum(null, EMPTY)); // a sentinel for termination

        PriorityQueue<Passage> passageQueue = new PriorityQueue<>(Math.min(64, maxPassages + 1), (left, right) -> {
            if (left.getScore() < right.getScore()) {
                return -1;
            } else if (left.getScore() > right.getScore()) {
                return 1;
            } else {
                return left.getStartOffset() - right.getStartOffset();
            }
        });
        Passage passage = new Passage(); // the current passage in-progress.  Will either get reset or added to queue.

        OffsetsEnum off;
        while ((off = offsetsEnumQueue.poll()) != null) {
            int start = off.startOffset();
            if (start == -1) {
                throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
            }
            int end = off.endOffset();
            // LUCENE-5166: this hit would span the content limit... however more valid
            // hits may exist (they are sorted by start). so we pretend like we never
            // saw this term, it won't cause a passage to be added to passageQueue or anything.
            assert EMPTY.startOffset() == Integer.MAX_VALUE;
            if (start < contentLength && end > contentLength) {
                continue;
            }
            // See if this term should be part of a new passage.
            if (start >= passage.getEndOffset()) {
                if (passage.getStartOffset() >= 0) { // true if this passage has terms; otherwise couldn't find any (yet)
                    // finalize passage
                    passage.setScore(passage.getScore() * scorer.norm(passage.getStartOffset()));
                    // new sentence: first add 'passage' to queue
                    if (passageQueue.size() == maxPassages && passage.getScore() < passageQueue.peek().getScore()) {
                        passage.reset(); // can't compete, just reset it
                    } else {
                        passageQueue.offer(passage);
                        if (passageQueue.size() > maxPassages) {
                            passage = passageQueue.poll();
                            passage.reset();
                        } else {
                            passage = new Passage();
                        }
                    }
                }
                // if we exceed limit, we are done
                if (start >= contentLength) {
                    break;
                }
                // advance breakIterator
                passage.setStartOffset(Math.max(breakIterator.preceding(start + 1), 0));
                passage.setEndOffset(Math.min(breakIterator.following(end - 1), contentLength));
            }
            // Add this term to the passage.
            int tf = 0;
            while (true) {
                tf++;
                BytesRef term = off.getTerm();// a reference; safe to refer to
                assert term != null;
                passage.addMatch(start, end, term);
                // see if there are multiple occurrences of this term in this passage. If so, add them.
                if (!off.hasMorePositions()) {
                    break; // No more in the entire text. Already removed from pq; move on
                }
                off.nextPosition();
                start = off.startOffset();
                end = off.endOffset();
                if (start >= passage.getEndOffset() || end > contentLength) { // it's beyond this passage
                    offsetsEnumQueue.offer(off);
                    break;
                }
            }
            passage.setScore(passage.getScore() + off.getWeight() * scorer.tf(tf, passage.getEndOffset() - passage.getStartOffset()));
        }

        Passage[] passages = passageQueue.toArray(new Passage[passageQueue.size()]);
        for (Passage p : passages) {
            p.sort();
        }
        // sort in ascending order
        Arrays.sort(passages, (left, right) -> left.getStartOffset() - right.getStartOffset());
        return passages;
    }
}
