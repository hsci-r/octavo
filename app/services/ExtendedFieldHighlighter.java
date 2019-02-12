package services;

import org.apache.lucene.search.uhighlight.*;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

// override class so highlightOffsetsEnum returns proper end offsets
public class ExtendedFieldHighlighter extends FieldHighlighter {

    public ExtendedFieldHighlighter(String field, FieldOffsetStrategy fieldOffsetStrategy, BreakIterator breakIterator,
                            PassageScorer passageScorer, int maxPassages, int maxNoHighlightPassages,
                            PassageFormatter passageFormatter) {
        super(field, fieldOffsetStrategy, breakIterator, passageScorer, maxPassages, maxNoHighlightPassages, passageFormatter);
    }

    // algorithm: treat sentence snippets as miniature documents
    // we can intersect these with the postings lists via BreakIterator.preceding(offset),s
    // score each sentence as norm(sentenceStartOffset) * sum(weight * tf(freq))
    protected Passage[] highlightOffsetsEnums(OffsetsEnum off)
            throws IOException {

        final int contentLength = this.breakIterator.getText().getEndIndex();

        if (off.nextPosition() == false) {
            return new Passage[0];
        }

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

        do {
            int start = off.startOffset();
            if (start == -1) {
                throw new IllegalArgumentException("field '" + field + "' was indexed without offsets, cannot highlight");
            }
            int end = off.endOffset();
            if (start < contentLength && end > contentLength) {
                continue;
            }
            // See if this term should be part of a new passage.
            if (start >= passage.getEndOffset()) {
                passage = maybeAddPassage(passageQueue, passageScorer, passage, contentLength);
                // if we exceed limit, we are done
                if (start >= contentLength) {
                    break;
                }
                // advance breakIterator
                passage.setStartOffset(Math.max(this.breakIterator.preceding(start + 1), 0));
                passage.setEndOffset(Math.min(this.breakIterator.following(end - 1), contentLength)); // CHANGED TO RETURN PROPER END OFFSET
            }
            // Add this term to the passage.
            BytesRef term = off.getTerm();// a reference; safe to refer to
            assert term != null;
            passage.addMatch(start, end, term, off.freq());
        } while (off.nextPosition());
        maybeAddPassage(passageQueue, passageScorer, passage, contentLength);

        Passage[] passages = passageQueue.toArray(new Passage[passageQueue.size()]);
        // sort in ascending order
        Arrays.sort(passages, Comparator.comparingInt(Passage::getStartOffset));
        return passages;
    }

    private Passage maybeAddPassage(PriorityQueue<Passage> passageQueue, PassageScorer scorer, Passage passage, int contentLength) {
        if (passage.getStartOffset() == -1) {
            // empty passage, we can ignore it
            return passage;
        }
        passage.setScore(scorer.score(passage, contentLength));
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
        return passage;
    }
}
