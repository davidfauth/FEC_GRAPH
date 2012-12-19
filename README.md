Follow the Data
===============

Basic importers for importing FEC Campaign Finance data into the [Neo4j](http://neo4j.org) Graph Database.

Requires
--------

- [Java](http://www.oracle.com/us/technologies/java/overview/index.html)
- [Apache Ant](http://ant.apache.org)

*Note*: that Java is just required for the initial batch import of data. The dataset can then 
be explored with Neo4j's own Cypher query language, or using one of the language drivers
listed below.

Follow these Steps
------------------

1. `git clone https://github.com/akollegger/FEC_GRAPH.git`
2. `cd FEC_GRAPH`
3. `ant initialize`
4. `ant`
5. `./bin/fec2graph --force --importer=[RAW|CONNECTED|RELATED|LIMITED]`
  - choose one of the importers, like `./bin/fec2graph --force --importer=RAW`
  - RAW: imports records with no modifications
6. `ant neo4j-start`

Indexed Nodes
-------------

- `candidates.CAND_ID`
- `candidates.CAND_NAME`
- `committees.CMTE_ID`
- `committees.CMTE_NM`

Sample queries using indexes:

`start cand=node:candidates(CAND_ID='P80003627')`
`start comm=node:candidates(CMTE_ID='C90012980')`

Cypher Challenge
----------------

Query for this...

// All presidential candidates for 2012

// Most mythical presidential candidate

// Top 10 Presidential candidates according to number of campaign committees

// find President Barack Obama

// lookup Obama by his candidate ID

// find Presidential Candidate Mitt Romney

// lookup Romney by his candidate ID

// find the shortest path of funding between Obama and Romney

// 10 top individual contributions to Obama

// 10 top individual contributions to Romney

*Hint*: New to all this? Here's how to identifiy one of the many fake candidates registered with the FEC.

After successfully listing all candidates for the first query, you could page through the listing to look for names that seem.. just off. Use limit and skip in the return clause to page through the long listing:

    start candidate=node:candidates('CAND_ID:*') 
    where candidate.CAND_OFFICE='{fill this in}' AND candidate.CAND_ELECTION_YR='{this too}' 
    return candidate.CAND_NAME skip 100 limit 100;

Once you spot one of the many candidate names that isn't real, you can query for it directly: 

    start candidate=node:candidates(CAND_NAME:'CLAUS, SANTA')
    return candidate;

To learn more about querying with Cypher, look to the excellent [Neo4j Manual](http://docs.neo4j.org).

Wanna code? Get a Neo4j Driver
------------------------------

- [Ruby - Neograph](https://github.com/maxdemarzi/neography)
- [Java - Spring Data Neo4j](http://www.springsource.org/spring-data/neo4j)
- [Javascript - Node Neo4js](https://github.com/thingdom/node-neo4j)
- [Neo4jPHP - PHP](https://github.com/jadell/Neo4jPHP/)
- [Bulbflow - Python](http://bulbflow.com)
- [Neo4Django - Django](https://github.com/scholrly/neo4django/)
- [Neo4jClient - .net](http://nuget.org/packages/Neo4jClient/)
- [Neo4j-GO - Google Go](https://github.com/davemeehan/Neo4j-GO)
- [Neocons - Clojure](http://clojureneo4j.info/)

References
----------

- [Neo4j](http://neo4j.org) - the graph database
- [Neo4j Cypher Reference](http://docs.neo4j.org/chunked/milestone/cypher-query-lang.html)
- [FEC Campaign Finance Data](http://www.fec.gov/finance/disclosure/ftpdet.shtml)

- [Candidate Record](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandidateMaster.shtml)
  - CAND_ID Candidate Identification
  - CAND_NAME Candidate Name
  - CAND_PTY_AFFILIATION Party Affiliation
  - CAND_ELECTION_YR Year of Election
  - CAND_OFFICE Candidate Office (P)resident, (S)enate, (H)ouse
  - CAND_ST State
  - CAND_CITY City
- [Committee Record](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteeMaster.shtml)
  - CMTE_ID Committee Identification
  - CMTE_NM Committee Name
  - CAND_ID Candidate Identification
- [Candidate Committee Linkage](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandCmteLinkage.shtml)
- [Committee to Committee Contributions](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteetoCommittee.shtml)
- [Committee to Candidates Contributions](http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionstoCandidates.shtml)

!["RELATED" model](https://raw.github.com/akollegger/FEC_GRAPH/master/FEC-model.gif)