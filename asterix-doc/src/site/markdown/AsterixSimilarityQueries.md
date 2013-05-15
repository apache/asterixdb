#summary Similarity Queries and Keyword Queries

`<wiki:toc max_depth="3" />`

# AsterixDB  Support of Similarity Queries #

## Motivation ##

Similarity queries are widely used in applications where users need to find records that satisfy a similarity predicate, while exact matching is not sufficient. These queries are especially important for social and Web applications, where errors, abbreviations, and inconsistencies are common.  As an example, we may want to find all the movies starring Schwarzenegger, while we don't know the exact spelling of his last name (despite his popularity in both the movie industry and politics :-)). As another example, we want to find all the Facebook users who have similar friends. To meet this type of needs, AsterixDB supports similarity queries using efficient indexes and algorithms.

## Data Types and Similarity Functions ##

AsterixDB supports various similarity functions, including [http://en.wikipedia.org/wiki/Levenshtein_distance edit distance] (on strings) and [http://en.wikipedia.org/wiki/Jaccard_index Jaccard] (on sets). For instance, in our [https://code.google.com/p/asterixdb/wiki/AdmAql101#ADM:_Modeling_Semistructed_Data_in_AsterixDB TinySocial] example, the `friend-ids` of a Facebook user forms a set of friends, and we can define a similarity between two sets. We can also convert a string to a set of "q-grams" and define the Jaccard similarity between the two sets of two strings. The "q-grams" of a string are its substrings of length "q". For instance, the 3-grams of the string `schwarzenegger` are `sch`, `chw`, `hwa`, ..., `ger`.

AsterixDB provides [https://code.google.com/p/asterixdb/wiki/AsterixDataTypesAndFunctions#Tokenizing_Functions tokenization functions] to convert strings to sets, and the [https://code.google.com/p/asterixdb/wiki/AsterixDataTypesAndFunctions#Similarity_Functions similarity functions].

## Selection Queries ##

The following [https://code.google.com/p/asterixdb/wiki/AsterixDataTypesAndFunctions#edit-distance query] asks for all the Facebook users whose name is similar to `Suzanna Tilson`, i.e., their edit distance is at most 2.


        use dataverse TinySocial;
        
        for $user in dataset('FacebookUsers')
        let $ed := edit-distance($user.name, "Suzanna Tilson")
        where $ed <= 2
        return $user


The following [https://code.google.com/p/asterixdb/wiki/AsterixDataTypesAndFunctions#similarity-jaccard query] asks for all the Facebook users whose set of friend ids is similar to `[1,5,9]`, i.e., their Jaccard similarity is at least 0.6.


        use dataverse TinySocial;
        
        for $user in dataset('FacebookUsers')
        let $sim := similarity-jaccard($user.friend-ids, [1,5,9])
        where $sim >= 0.6f
        return $user


AsterixDB allows a user to use a similarity operator `~=` to express a similarity condition by defining the similiarty function and threshold using "set" statements earlier. For instance, the above query can be equivalently written as:


        use dataverse TinySocial;
        
        set simfunction "jaccard";
        set simthreshold "0.6f";
        
        for $user in dataset('FacebookUsers')
        where $user.friend-ids ~= [1,5,9]
        return $user



## Fuzzy Join Queries ##

AsterixDB supports fuzzy joins between two data sets. The following [https://code.google.com/p/asterixdb/wiki/AdmAql101#Query_5_-_Fuzzy_Join query] finds, for each Facebook user, all Twitter users with names "similar" to their name based on the edit distance.


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


## Using Indexes ##

AsterixDB uses inverted index to support similarity queries efficiently. For instance, the following query creates such an index on the `FacebookUser.name` attribute using an inverted index of 3-grams.  After the index is created, similarity queries with an edit distance condition on this attribute can be answered more efficiently.


        use dataverse TinySocial;
        
        create index fbUserFuzzyIdx on FacebookUsers(name) type ngram(3);

