package services;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.search.*;
import org.apache.lucene.search.uhighlight.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendedUnifiedHighlighter extends UnifiedHighlighter {

    private Logger logger = LoggerFactory.getLogger(ExtendedUnifiedHighlighter.class);
    public static final IndexSearcher EMPTY_INDEXSEARCHER;

    static {
        try {
            IndexReader emptyReader = new MultiReader();
            EMPTY_INDEXSEARCHER = new IndexSearcher(emptyReader);
            EMPTY_INDEXSEARCHER.setQueryCache(null);
        } catch (IOException bogus) {
            throw new RuntimeException(bogus);
        }
    }

    public static class Passages {
        public final Passage[] passages;
        public final String content;

        Passages(Passage[] passages, String content) {
            this.passages=passages;
            this.content=content;
        }

    }

    private final boolean matchFullSpans;

    public ExtendedUnifiedHighlighter(IndexSearcher indexSearcher, Analyzer indexAnalyzer, boolean matchFullSpans) {
        super(indexSearcher, indexAnalyzer);
        this.matchFullSpans = matchFullSpans;
        setFormatter(new PassageFormatter() {
            @Override
            public Object format(Passage[] passages, String content) {
                return new Passages(passages,content);
            }
        });
        setMaxLength(Integer.MAX_VALUE - 1);
    }

    public static final DefaultPassageFormatter defaultPassageFormatter = new DefaultPassageFormatter();

    public Passages[] highlightAsPassages(String field, Query query, int[] docIds, int maxPassages) {
        try {
            Object[] ret = highlightFieldsAsObjects(new String[]{field}, query, docIds, new int[]{maxPassages}).get(field);
            return Arrays.copyOf(ret, ret.length, Passages[].class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> highlightsToStrings(Passages p, boolean removeNonMatches) {
        Passage[] single = new Passage[1];
        List<String> ret = new ArrayList<>(p.passages.length);
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
    protected PhraseHelper getPhraseHelper(String field, Query query, Set<HighlightFlag> highlightFlags) {
        boolean useWeightMatchesIter = highlightFlags.contains(HighlightFlag.WEIGHT_MATCHES);
        if (useWeightMatchesIter) {
            return PhraseHelper.NONE; // will be handled by Weight.matches which always considers phrases
        }
        boolean highlightPhrasesStrictly = highlightFlags.contains(HighlightFlag.PHRASES);
        boolean handleMultiTermQuery = highlightFlags.contains(HighlightFlag.MULTI_TERM_QUERY);
        return highlightPhrasesStrictly ?
                new ExtendedPhraseHelper(query, field, getFieldMatcher(field),
                        this::requiresRewrite,
                        this::preSpanQueryRewrite,
                        !handleMultiTermQuery,
                        matchFullSpans
                )
                : PhraseHelper.NONE;
    }

    @Override
    protected boolean hasUnrecognizedQuery(Predicate<String> fieldMatcher, Query query) {
        if (query instanceof NumericDocValuesWeightedMatchAllDocsQuery) return false;
        return super.hasUnrecognizedQuery(fieldMatcher, query);
    }

    @Override
    protected FieldHighlighter getFieldHighlighter(String field, Query iquery, Set<Term> allTerms, int maxPassages) {
        try {
            Query query = searcher.rewrite(iquery);
            allTerms.clear();
            query.visit(new QueryVisitor() {

                public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
                    if (occur == BooleanClause.Occur.FILTER)
                        return EMPTY_VISITOR;
                    return super.getSubVisitor(occur, parent);
                }

                @Override
                public void consumeTerms(Query query, Term... terms) {
                    Collections.addAll(allTerms, terms);
                }
            }); // needs to be redone here because superclass uses an empty indexsearcher, which doesn't work with complex phrase queries. Also, we'll filter out FILTER parts of the query
            UHComponents components = getHighlightComponents(field, query, allTerms);
            if (components.hasUnrecognizedQueryPart())
                logger.warn(query+" has unrecognised query part(s)");
            OffsetSource offsetSource = getOptimizedOffsetSource(components);
            if (offsetSource == OffsetSource.POSTINGS_WITH_TERM_VECTORS) {
                if (components.getAutomata().length > 0) offsetSource = OffsetSource.ANALYSIS;
                else {
                    hack = true;
                    OffsetSource offsetSource2 = getOptimizedOffsetSource(components);
                    hack = false;
                    if (offsetSource2 == OffsetSource.ANALYSIS) offsetSource = OffsetSource.ANALYSIS;
                }
            }
            return new FieldHighlighter(field,
                    getOffsetStrategy(offsetSource, components),
                    new SplittingBreakIterator(getBreakIterator(field), UnifiedHighlighter.MULTIVAL_SEP_CHAR),
                    getScorer(field),
                    maxPassages,
                    getMaxNoHighlightPassages(field),
                    getFormatter(field));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hack = false;

    @Override
    protected OffsetSource getOffsetSource(String field) {
        if (hack) return OffsetSource.POSTINGS;
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

    @Override
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
