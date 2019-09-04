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

# AsterixDB  Support of Full-text search queries #

## <a id="toc">Table of Contents</a> ##

* [Motivation](#Motivation)
* [Syntax](#Syntax)
* [Creating and utilizing a Full-text index](#FulltextIndex)

## <a id="Motivation">Motivation</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

Full-Text Search (FTS) queries are widely used in applications where users need to find records that satisfy
an FTS predicate, i.e., where simple string-based matching is not sufficient. These queries are important when
finding documents that contain a certain keyword is crucial. FTS queries are different from substring matching
queries in that FTS queries find their query predicates as exact keywords in the given string, rather than
treating a query predicate as a sequence of characters. For example, an FTS query that finds “rain” correctly
returns a document when it contains “rain” as a word. However, a substring-matching query returns a document
whenever it contains “rain” as a substring, for instance, a document with “brain” or “training” would be
returned as well.

## <a id="Syntax">Syntax</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

The syntax of AsterixDB FTS follows a portion of the XQuery FullText Search syntax.
Two basic forms are as follows:

        ftcontains(Expression1, Expression2, {FullTextOption})
        ftcontains(Expression1, Expression2)

For example, we can execute the following query to find Chirp messages where the `messageText` field includes
“voice” as a word. Please note that an FTS search is case-insensitive.
Thus, "Voice" or "voice" will be evaluated as the same word.

        use TinySocial;

        select element {"chirpId": msg.chirpId}
        from ChirpMessages msg
        where ftcontains(msg.messageText, "voice", {"mode":"any"});

The DDL and DML of TinySocial can be found in [ADM: Modeling Semistructured Data in AsterixDB](../sqlpp/primer-sqlpp.html#ADM:_Modeling_Semistructured_Data_in_AsterixDB).

The `Expression1` is an expression that should be evaluable as a string at runtime as in the above example
where `msg.messageText` is a string field. The `Expression2` can be a string, an (un)ordered list
of string value(s), or an expression. In the last case, the given expression should be evaluable
into one of the first two types, i.e., into a string value or an (un)ordered list of string value(s).

The following examples are all valid expressions.

       ... where ftcontains(msg.messageText, "sound")
       ... where ftcontains(msg.messageText, "sound", {"mode":"any"})
       ... where ftcontains(msg.messageText, ["sound", "system"], {"mode":"any"})
       ... where ftcontains(msg.messageText, {{"speed", "stand", "customization"}}, {"mode":"all"})

The last `FullTextOption` parameter clarifies the given FTS request. If you omit the `FullTextOption` parameter,
then the default value will be set for each possible option. Currently, we only have one option named `mode`.
And as we extend the FTS feature, more options will be added. Please note that the format of `FullTextOption`
is a record, thus you need to put the option(s) in a record `{}`.
The `mode` option indicates whether the given FTS query is a conjunctive (AND) or disjunctive (OR) search request.
This option can be either `“all”` (AND) or `“any”` (OR). The default value for `mode` is `“all”`. If one specifies `“any”`,
a disjunctive search will be conducted. For example, the following query will find documents whose `messageText`
field contains “sound” or “system”, so a document will be returned if it contains either “sound”, “system”,
or both of the keywords.

       ... where ftcontains(msg.messageText, ["sound", "system"], {"mode":"any"})

The other option parameter,`“all”`, specifies a conjunctive search. The following examples will find the documents whose
`messageText` field contains both “sound” and “system”. If a document contains only “sound” or “system” but
not both, it will not be returned.

       ... where ftcontains(msg.messageText, ["sound", "system"], {"mode":"all"})
       ... where ftcontains(msg.messageText, ["sound", "system"])

Currently AsterixDB doesn’t (yet) support phrase searches, so the following query will not work.

       ... where ftcontains(msg.messageText, "sound system", {"mode":"any"})

As a workaround solution, the following query can be used to achieve a roughly similar goal. The difference is that
the following queries will find documents where `msg.messageText` contains both “sound” and “system”, but the order
and adjacency of “sound” and “system” are not checked, unlike in a phrase search. As a result, the query below would
also return documents with “sound system can be installed.”, “system sound is perfect.”,
or “sound is not clear. You may need to install a new system.”

       ... where ftcontains(msg.messageText, ["sound", "system"], {"mode":"all"})
       ... where ftcontains(msg.messageText, ["sound", "system"])


## <a id="FulltextIndex">Creating and utilizing a Full-text index</a> <font size="4"><a href="#toc">[Back to TOC]</a></font> ##

When there is a full-text index on the field that is being searched, rather than scanning all records,
AsterixDB can utilize that index to expedite the execution of a FTS query. To create a full-text index,
you need to specify the index type as `fulltext` in your DDL statement. For instance, the following DDL
statement create a full-text index on the `GleambookMessages.message` attribute. Note that a full-text index
cannot be built on a dataset with the variable-length primary key (e.g., string).

    use TinySocial;

    create index messageFTSIdx on GleambookMessages(message) type fulltext;
