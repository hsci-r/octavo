package services;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.WeightedSpanTerm;
import org.apache.lucene.search.uhighlight.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.search.uhighlight.UnifiedHighlighter.OffsetSource;

public class ExtendedUnifiedHighlighter extends UnifiedHighlighter {

    public static class Passages {
        public final Passage[] passages;
        public final String content;

        Passages(Passage[] passages, String content) {
            this.passages=passages;
            this.content=content;
        }

    }

    public ExtendedUnifiedHighlighter(IndexSearcher indexSearcher, Analyzer indexAnalyzer) {
        super(indexSearcher, indexAnalyzer);
        this.setFormatter(new PassageFormatter() {
            @Override
            public Object format(Passage[] passages, String content) {
                return new Passages(passages,content);
            }
        });
        setMaxLength(Integer.MAX_VALUE - 1);
    }

    public static final DefaultPassageFormatter defaultPassageFormatter = new DefaultPassageFormatter();

    public Passages[] highlight(String field, Query query, int[] docIds, int maxPassages) throws IOException  {
        Object[] ret = highlightFieldsAsObjects(new String[]{field}, query, docIds, new int[]{maxPassages}).get(field);
        return Arrays.copyOf(ret, ret.length, Passages[].class);
    }

    public static List<String> highlightsToStrings(Passages p, boolean removeNonMatches) {
        Passage[] single = new Passage[1];
        List<String> ret = new ArrayList<String>(p.passages.length);
        for (Passage pas: p.passages) if (!removeNonMatches || pas.getNumMatches()>0) {
            single[0] = pas;
            ret.add(defaultPassageFormatter.format(single, p.content));
        }
        return ret;
    }

    public static String highlightToString(Passage pas, String content) {
        return defaultPassageFormatter.format(new Passage[] { pas }, content);
    }

    @Override
    protected FieldHighlighter getFieldHighlighter(String field, Query query, Set<Term> allTerms, int maxPassages) {
        try {
            searcher.createNormalizedWeight(query, false).extractTerms(allTerms); // needs to be redone here because superclass uses an empty indexsearcher, which doesn't work with complex phrase queries.
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    protected Collection<Query> preSpanQueryRewrite(Query query) {
        if (query == null) return null;
        else if ("org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser.ComplexPhraseQuery".equals(query.getClass().getCanonicalName())) try {
            return Collections.singleton(query.rewrite(getIndexSearcher().getIndexReader()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

}
