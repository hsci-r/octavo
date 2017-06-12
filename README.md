# Octavo - Open Interface for Text and Metadata

Octavo is a JSON API -based server for querying and analyzing textual collections with attendant metadata. It has been used to store and access for example:
 * Eighteenth Century Collections Online
 * British Library Newspapers / Burney Collection Newspapers
 * News articles from YLE and Helsingin Sanomat
 * The Finnish Digitized Newspaper Collection

The primary functionalities provided by Octavo are robust querying and constraining capabilities based on an expanded form of the Lucene query syntax (e.g. return all _books_ containing the word _consciousness_, but only in the _preface section_, and only if the book is _longer than 50 pages_, and only if the book also contains the word _philosophy_ or _religion_, or return all _sentences_ where the lemma _"Juha Sipilä"_ appears with an _adjective_ at most _3 words apart_).

On top of this search functionality, Octavo also provides performant support for collocation analysis based on the queries (to discover e.g. which words are associated with the query _energy policy_ in articles _written at YLE_ by someone _other than the YLE news desk_, grouped by _year_).

All of the above queries can be done on various levels, e.g. by sentence, paragraph, section or whole work, depending on what answering the end user's question requires.

## Levels of operation

Octavo has been designed to as flexibly as possible support many different levels of operation, depending on the needs of the end user workflow.

For example, a custom data science workflow can just retrieve all complete documents (or e.g. their paragraphs) matching a particular criteria, for complete control over local processing.

On the other hand, for e.g. collocation-type queries, it makes sense to use the efficient Octavo index. To customize this workflow, multiple ready options are provided, but also Groovy scripts can be injected into the processing of such queries to further open new options.

Finally, there are also [ready user interfaces](https://jiemakel.github.io/octavo-ui/) built on top of Octavo for the most common use cases.
