
# AsterixDB  Support of Similarity Queries #

## Motivation ##

Similarity queries are widely used in applications where users need to
find records that satisfy a similarity predicate, while exact matching
is not sufficient. These queries are especially important for social
and Web applications, where errors, abbreviations, and inconsistencies
are common.  As an example, we may want to find all the movies
starring Schwarzenegger, while we don't know the exact spelling of his
last name (despite his popularity in both the movie industry and
politics :-)). As another example, we want to find all the Facebook
users who have similar friends. To meet this type of needs, AsterixDB
supports similarity queries using efficient indexes and algorithms.

## Data Types and Similarity Functions ##

AsterixDB supports [edit distance](http://en.wikipedia.org/wiki/Levenshtein_distance) (on strings) and
[Jaccard](http://en.wikipedia.org/wiki/Jaccard_index) (on sets).  For
instance, in our
[TinySocial](primer.html#ADM:_Modeling_Semistructed_Data_in_AsterixDB)
example, the `friend-ids` of a Facebook user forms a set
of friends, and we can define a similarity between the sets of
friends of two users. We can also convert a string to a set of grams of a length "n"
(called "n-grams") and define the Jaccard similarity between the two
gram sets of the two strings. Formally, the "n-grams" of a string are
its substrings of length "n". For instance, the 3-grams of the string
`schwarzenegger` are `sch`, `chw`, `hwa`, ..., `ger`.

AsterixDB provides
[tokenization functions](functions.html#Tokenizing_Functions)
to convert strings to sets, and the
[similarity functions](functions.html#Similarity_Functions).

## Similarity Selection Queries ##

The following [query](functions.html#edit-distance)
asks for all the Facebook users whose name is similar to
`Suzanna Tilson`, i.e., their edit distance is at most 2.

        use dataverse TinySocial;
        
        for $user in dataset('FacebookUsers')
        let $ed := edit-distance($user.name, "Suzanna Tilson")
        where $ed <= 2
        return $user


The following [query](functions.html#similarity-jaccard)
asks for all the Facebook users whose set of friend ids is
similar to `[1,5,9]`, i.e., their Jaccard similarity is at least 0.6.

        use dataverse TinySocial;
        
        for $user in dataset('FacebookUsers')
        let $sim := similarity-jaccard($user.friend-ids, [1,5,9])
        where $sim >= 0.6f
        return $user


AsterixDB allows a user to use a similarity operator `~=` to express a
condition by defining the similarity function and threshold
using "set" statements earlier. For instance, the above query can be
equivalently written as:

        use dataverse TinySocial;
        
        set simfunction "jaccard";
        set simthreshold "0.6f";
        
        for $user in dataset('FacebookUsers')
        where $user.friend-ids ~= [1,5,9]
        return $user


In this query, we first declare Jaccard as the similarity function
using `simfunction` and then specify the threshold `0.6f` using
`simthreshold`.

## Similarity Join Queries ##

AsterixDB supports fuzzy joins between two sets. The following
[query](primer.html#Query_5_-_Fuzzy_Join)
finds, for each Facebook user, all Twitter users with names
similar to their name based on the edit distance.

        use dataverse TinySocial;
        
        set simfunction "edit-distance";
        set simthreshold "3";
	
        for $fbu in dataset FacebookUsers
        return {
            "id": $fbu.id,
            "name": $fbu.name,
            "similar-users": for $t in dataset TweetMessages
                                let $tu := $t.user
                                where $tu.name ~= $fbu.name
                                return {
                                "twitter-screenname": $tu.screen-name,
                                "twitter-name": $tu.name
                                }
        };

## Using Indexes to Support Similarity Queries ##

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
`FacebookUsers.name` attribute using an inverted index of 3-grams.

        use dataverse TinySocial;
        
        create index fbUserIdx on FacebookUsers(name) type ngram(3);

The number "3" in "ngram(3)" is the length "n" in the grams. This
index can be used to optimize similarity queries on this attribute
using 
[edit-distance](functions.html#edit-distance), 
[edit-distance-check](functions.html#edit-distance-check), 
[jaccard](functions.html#similarity-jaccard),
or [jaccard-check](functions.html#similarity-jaccard-check) 
queries on this attribute where the
similarity is defined on sets of 3-grams.  This index can also be used
to optimize queries with the "[contains()]((functions.html#contains))" predicate (i.e., substring
matching) since it can be also be solved by counting on the inverted
lists of the grams in the query string.

### Keyword Index ###

A "keyword index" is constructed on a set of strings or sets (e.g., OrderedList, UnorderedList). Instead of 
generating grams as in an ngram index, we generate tokens (e.g., words) and for each token, construct an inverted list that includes the ids of the
records with this token.  The following two examples show how to create keyword index on two different types:


#### Keyword Index on String Type ####

        use dataverse TinySocial;

        create index fbMessageIdx on FacebookMessages(message) type keyword;

        for $o in dataset('FacebookMessages')
        let $jacc := similarity-jaccard-check(word-tokens($o.message), word-tokens("love like verizon"), 0.2f)
        where $jacc[0]
        return $o
        
#### Keyword Index on UnorderedList Type ####
        
        use dataverse TinySocial;

        create index fbUserIdx_fids on FacebookUsers(friend-ids) type keyword;

        for $c in dataset('FacebookUsers')
        let $jacc := similarity-jaccard-check($c.friend-ids, {{3,10}}, 0.5f)
        where $jacc[0]
        return $c
        
As shown above, keyword index can be used to optimize queries with token-based similarity predicates, including
[similarity-jaccard](functions.html#similarity-jaccard) and
[similarity-jaccard-check](functions.html#similarity-jaccard-check).

