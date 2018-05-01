package services;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.uhighlight.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.search.uhighlight.UnifiedHighlighter.OffsetSource;

public class ExtendedUnifiedHighlighter extends UnifiedHighlighter {

    private final PassageFormatter defaultPassageFormatter = new DefaultPassageFormatter();

    public ExtendedUnifiedHighlighter(IndexSearcher indexSearcher, Analyzer indexAnalyzer) {
        super(indexSearcher, indexAnalyzer);
        this.setFormatter(new PassageFormatter() {
            @Override
            public Object format(Passage[] passages, String content) {
                String[] ret = new String[passages.length];
                Passage[] single = new Passage[1];
                for (int i =0;i < passages.length;i++) {
                    single[0] = passages[i];
                    ret[i]=(String)defaultPassageFormatter.format(single,content);
                }
                return ret;
            }
        });
        setMaxLength(Integer.MAX_VALUE - 1);
    }

    public String[][] highlight(String field, Query query, int[] docIds, int maxPassages) throws IOException  {
        Object[] ret =  highlightFieldsAsObjects(new String[]{field}, query, docIds, new int[]{maxPassages}).get(field);
        String[][] ret2 = new String[ret.length][];
        for (int i=0;i<ret.length;i++) ret2[i]=(String[])ret[i];
        return ret2;
    }

    @Override
    protected FieldHighlighter getFieldHighlighter(String field, Query query, Set<Term> allTerms, int maxPassages) {
        BytesRef[] terms = filterExtractedTerms(getFieldMatcher(field), allTerms);
        Set<HighlightFlag> highlightFlags = getFlags(field);
        PhraseHelper phraseHelper = getPhraseHelper(field, query, highlightFlags);
        CharacterRunAutomaton[] automata = getAutomata(field, query, highlightFlags);
        OffsetSource offsetSource = getOptimizedOffsetSource(field, terms, phraseHelper, automata);
        return new ExtendedFieldHighlighter(field,
                getOffsetStrategy(offsetSource, field, terms, phraseHelper, automata, highlightFlags),
                new SplittingBreakIterator(getBreakIterator(field), UnifiedHighlighter.MULTIVAL_SEP_CHAR),
                getScorer(field),
                maxPassages,
                getMaxNoHighlightPassages(field),
                getFormatter(field));
    }

    protected OffsetSource getOffsetSource(String field) {
        FieldInfo fieldInfo = getFieldInfo(field);
        if (fieldInfo != null) {
            if (fieldInfo.getIndexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) {
                return fieldInfo.hasVectors() ? OffsetSource.POSTINGS_WITH_TERM_VECTORS : OffsetSource.POSTINGS;
            }
            /* if (fieldInfo.hasVectors()) { // unfortunately we can't also check if the TV has offsets
                return OffsetSource.TERM_VECTORS;
            } */
        }
        return OffsetSource.ANALYSIS;
    }
}
