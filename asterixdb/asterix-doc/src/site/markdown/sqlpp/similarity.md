<!--
 ! Licensed to the Apache Software Foundation (ASF) under one
 ! or more contributor license agreements.  See the NOTICE file
 ! distributed with this work for additional information
 ! regarding copyright ownership.  The ASF licenses this file
 ! to you under the Apache License, Version 2.0 (the
 ! "License"); you may not use this file except in compliance
 ! with the License.  You may obtain a copy of the License at
 !
 !   http://www.apache.org/licenses/LICENSE-2.0
 !
 ! Unless required by applicable law or agreed to in writing,
 ! software distributed under the License is distributed on an
 ! "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 ! KIND, either express or implied.  See the License for the
 ! specific language governing permissions and limitations
 ! under the License.
 !-->

# AsterixDB  Support of Similarity Queries #

## <a id="toc">Table of Contents</a> ##

* [Motivation](#Motivation)
* [Data Types and Similarity Functions](#DataTypesAndSimilarityFunctions)
* [Similarity Selection Queries](#SimilaritySelectionQueries)
* [Similarity Join Queries](#SimilarityJoinQueries)
* [Using Indexes to Support Similarity Queries](#UsingIndexesToSupportSimilarityQueries)

## <a id="Motivation">Motivation</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

Similarity queries are widely used in applications where users need to
find objects that satisfy a similarity predicate, while exact matching
is not sufficient. These queries are especially important for social
and Web applications, where errors, abbreviations, and inconsistencies
are common.  As an example, we may want to find all the movies
starring Schwarzenegger, while we don't know the exact spelling of his
last name (despite his popularity in both the movie industry and
politics :-)). As another example, we want to find all the Facebook
users who have similar friends. To meet this type of needs, AsterixDB
supports similarity queries using efficient indexes and algorithms.

## <a id="DataTypesAndSimilarityFunctions">Data Types and Similarity Functions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

AsterixDB supports [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) (on strings) and
[Jaccard](http://en.wikipedia.org/wiki/Jaccard_index) (on sets).  For
instance, in our
[TinySocial](../sqlpp/primer-sqlpp.html#ADM:_Modeling_Semistructured_Data_in_AsterixDB)
example, the `friendIds` of a Gleambook user forms a set
of friends, and we can define a similarity between the sets of
friends of two users. We can also convert a string to a set of grams of a length "n"
(called "n-grams") and define the Jaccard similarity between the two
gram sets of the two strings. Formally, the "n-grams" of a string are
its substrings of length "n". For instance, the 3-grams of the string
`schwarzenegger` are `sch`, `chw`, `hwa`, ..., `ger`.

AsterixDB provides
[tokenization functions](../sqlpp/builtins.html#Tokenizing_Functions)
to convert strings to sets, and the
[similarity functions](../sqlpp/builtins.html#Similarity_Functions).

## <a id="SimilaritySelectionQueries">Similarity Selection Queries</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

The following query
asks for all the Gleambook users whose name is similar to
`Suzanna Tilson`, i.e., their edit distance is at most 2.

        use TinySocial;

        select u
        from GleambookUsers u
        where edit_distance(u.name, "Suzanna Tilson") <= 2;

The following query
asks for all the Gleambook users whose set of friend ids is
similar to `[1,5,9,10]`, i.e., their Jaccard similarity is at least 0.6.

        use TinySocial;

        select u
        from GleambookUsers u
        where similarity_jaccard(u.friendIds, [1,5,9,10]) >= 0.6f;

AsterixDB allows a user to use a similarity operator `~=` to express a
condition by defining the similarity function and threshold
using "set" statements earlier. For instance, the above query can be
equivalently written as:

        use TinySocial;

        set simfunction "jaccard";
        set simthreshold "0.6f";

        select u
        from GleambookUsers u
        where u.friendIds ~= [1,5,9,10];

In this query, we first declare Jaccard as the similarity function
using `simfunction` and then specify the threshold `0.6f` using
`simthreshold`.

## <a id="SimilarityJoinQueries">Similarity Join Queries</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

AsterixDB supports fuzzy joins between two sets. The following
[query](../sqlpp/primer-sqlpp.html#Query_5_-_Fuzzy_Join)
finds, for each Gleambook user, all Chirp users with names
similar to their name based on the edit distance.

        use TinySocial;

        set simfunction "edit-distance";
        set simthreshold "3";

        select gbu.id, gbu.name, (select cu.screenName, cu.name
                                  from ChirpUsers cu
                                  where cu.name ~= gbu.name) as similar_users
        from GleambookUsers gbu;

## <a id="UsingIndexesToSupportSimilarityQueries">Using Indexes to Support Similarity Queries</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

AsterixDB uses two types of indexes to support similarity queries, namely
"ngram index" and "keyword index".

### NGram Index ###

An "ngram index" is constructed on a set of strings.  We generate n-grams for each string, and build an inverted
list for each n-gram that includes the ids of the strings with this
gram.  A similarity query can be answered efficiently by accessing the
inverted lists of the grams in the query and counting the number of
occurrences of the string ids on these inverted lists.  The similar
idea can be used to answer queries with Jaccard similarity.  A
detailed description of these techniques is available at this
[paper](http://www.ics.uci.edu/~chenli/pub/icde2009-memreducer.pdf).

For instance, the following DDL statements create an ngram index on the
`GleambookUsers.name` attribute using an inverted index of 3-grams.

        use TinySocial;

        create index gbUserIdx on GleambookUsers(name) type ngram(3);

The number "3" in "ngram(3)" is the length "n" in the grams. This
index can be used to optimize similarity queries on this attribute
using
[edit_distance](../sqlpp/builtins.html#edit_distance),
[edit_distance_check](../sqlpp/builtins.html#edit_distance_check),
[similarity_jaccard](../sqlpp/builtins.html#similarity_jaccard),
or [similarity_jaccard_check](../sqlpp/builtins.html#similarity_jaccard_check)
queries on this attribute where the
similarity is defined on sets of 3-grams.  This index can also be used
to optimize queries with the "[contains()]((../sqlpp/builtins.html#contains))" predicate (i.e., substring
matching) since it can be also be solved by counting on the inverted
lists of the grams in the query string.

#### NGram Index usage case - [edit_distance](../sqlpp/builtins.html#edit-distance) ####

        use TinySocial;

        select u
        from GleambookUsers u
        where edit_distance(u.name, "Suzanna Tilson") <= 2;

#### NGram Index usage case - [edit_distance_check](../sqlpp/builtins.html#edit_distance_check) ####

        use TinySocial;

        select u
        from GleambookUsers u
        where edit_distance_check(u.name, "Suzanna Tilson", 2)[0];

#### NGram Index usage case - [contains()]((../sqlpp/builtins.html#contains)) ####

        use TinySocial;

        select m
        from GleambookMessages m
        where contains(m.message, "phone");


### Keyword Index ###

A "keyword index" is constructed on a set of strings or sets (e.g., array, multiset). Instead of
generating grams as in an ngram index, we generate tokens (e.g., words) and for each token, construct an inverted list that includes the ids of the
objects with this token.  The following two examples show how to create keyword index on two different types:


#### Keyword Index on String Type ####

        use TinySocial;

        drop index GleambookMessages.gbMessageIdx if exists;
        create index gbMessageIdx on GleambookMessages(message) type keyword;

        select m
        from GleambookMessages m
        where similarity_jaccard_check(word_tokens(m.message), word_tokens("love like ccast"), 0.2f)[0];

#### Keyword Index on Multiset Type ####

        use TinySocial;

        create index gbUserIdxFIds on GleambookUsers(friendIds) type keyword;

        select u
        from GleambookUsers u
        where similarity_jaccard_check(u.friendIds, {{3,10}}, 0.5f)[0];

As shown above, keyword index can be used to optimize queries with token-based similarity predicates, including
[similarity_jaccard](../sqlpp/builtins.html#similarity_jaccard) and
[similarity_jaccard_check](../sqlpp/builtins.html#similarity_jaccard_check).

#### Keyword Index usage case - [similarity_jaccard](../sqlpp/builtins.html#similarity_jaccard) ####

        use TinySocial;

        select u
        from GleambookUsers u
        where similarity_jaccard(u.friendIds, [1,5,9,10]) >= 0.6f;

#### Keyword Index usage case - [similarity_jaccard_check](../sqlpp/builtins.html#similarity_jaccard_check) ####

        use TinySocial;

        select u
        from GleambookUsers u
        where similarity_jaccard_check(u.friendIds, [1,5,9,10], 0.6f)[0];

