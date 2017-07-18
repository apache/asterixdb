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

A SQL++ query can be any legal SQL++ expression or `SELECT` statement. A SQL++ query always ends with a semicolon.

    Query ::= (Expression | SelectStatement) ";"

##  <a id="SELECT_statements">SELECT Statements</a>

The following shows the (rich) grammar for the `SELECT` statement in SQL++.

    SelectStatement    ::= ( WithClause )?
                           SelectSetOperation (OrderbyClause )? ( LimitClause )?
    SelectSetOperation ::= SelectBlock (<UNION> <ALL> ( SelectBlock | Subquery ) )*
    Subquery           ::= "(" SelectStatement ")"

    SelectBlock        ::= SelectClause
                           ( FromClause ( LetClause )?)?
                           ( WhereClause )?
                           ( GroupbyClause ( LetClause )? ( HavingClause )? )?
                           |
                           FromClause ( LetClause )?
                           ( WhereClause )?
                           ( GroupbyClause ( LetClause )? ( HavingClause )? )?
                           SelectClause

    SelectClause       ::= <SELECT> ( <ALL> | <DISTINCT> )? ( SelectRegular | SelectValue )
    SelectRegular      ::= Projection ( "," Projection )*
    SelectValue      ::= ( <VALUE> | <ELEMENT> | <RAW> ) Expression
    Projection         ::= ( Expression ( <AS> )? Identifier | "*" )

    FromClause         ::= <FROM> FromTerm ( "," FromTerm )*
    FromTerm           ::= Expression (( <AS> )? Variable)?
                           ( ( JoinType )? ( JoinClause | UnnestClause ) )*

    JoinClause         ::= <JOIN> Expression (( <AS> )? Variable)? <ON> Expression
    UnnestClause       ::= ( <UNNEST> | <CORRELATE> | <FLATTEN> ) Expression
                           ( <AS> )? Variable ( <AT> Variable )?
    JoinType           ::= ( <INNER> | <LEFT> ( <OUTER> )? )

    WithClause         ::= <WITH> WithElement ( "," WithElement )*
    LetClause          ::= (<LET> | <LETTING>) LetElement ( "," LetElement )*
    LetElement         ::= Variable "=" Expression
    WithElement        ::= Variable <AS> Expression

    WhereClause        ::= <WHERE> Expression

    GroupbyClause      ::= <GROUP> <BY> ( Expression ( (<AS>)? Variable )? ( "," Expression ( (<AS>)? Variable )? )*
                           ( <GROUP> <AS> Variable
                             ("(" Variable <AS> VariableReference ("," Variable <AS> VariableReference )* ")")?
                           )?
    HavingClause       ::= <HAVING> Expression

    OrderbyClause      ::= <ORDER> <BY> Expression ( <ASC> | <DESC> )? ( "," Expression ( <ASC> | <DESC> )? )*
    LimitClause        ::= <LIMIT> Expression ( <OFFSET> Expression )?

In this section, we will make use of two stored collections of objects (datasets), `GleambookUsers` and `GleambookMessages`, in a series of running examples to explain `SELECT` queries. The contents of the example collections are as follows:

`GleambookUsers` collection (or, dataset):

    [ {
      "id":1,
      "alias":"Margarita",
      "name":"MargaritaStoddard",
      "nickname":"Mags",
      "userSince":"2012-08-20T10:10:00",
      "friendIds":[2,3,6,10],
      "employment":[{
                      "organizationName":"Codetechno",
                      "start-date":"2006-08-06"
                    },
                    {
                      "organizationName":"geomedia",
                      "start-date":"2010-06-17",
                      "end-date":"2010-01-26"
                    }],
      "gender":"F"
    },
    {
      "id":2,
      "alias":"Isbel",
      "name":"IsbelDull",
      "nickname":"Izzy",
      "userSince":"2011-01-22T10:10:00",
      "friendIds":[1,4],
      "employment":[{
                      "organizationName":"Hexviafind",
                      "startDate":"2010-04-27"
                   }]
    },
    {
      "id":3,
      "alias":"Emory",
      "name":"EmoryUnk",
      "userSince":"2012-07-10T10:10:00",
      "friendIds":[1,5,8,9],
      "employment":[{
                      "organizationName":"geomedia",
                      "startDate":"2010-06-17",
                      "endDate":"2010-01-26"
                   }]
    } ]

`GleambookMessages` collection (or, dataset):

    [ {
      "messageId":2,
      "authorId":1,
      "inResponseTo":4,
      "senderLocation":[41.66,80.87],
      "message":" dislike x-phone its touch-screen is horrible"
    },
    {
      "messageId":3,
      "authorId":2,
      "inResponseTo":4,
      "senderLocation":[48.09,81.01],
      "message":" like product-y the plan is amazing"
    },
    {
      "messageId":4,
      "authorId":1,
      "inResponseTo":2,
      "senderLocation":[37.73,97.04],
      "message":" can't stand acast the network is horrible:("
    },
    {
      "messageId":6,
      "authorId":2,
      "inResponseTo":1,
      "senderLocation":[31.5,75.56],
      "message":" like product-z its platform is mind-blowing"
    }
    {
      "messageId":8,
      "authorId":1,
      "inResponseTo":11,
      "senderLocation":[40.33,80.87],
      "message":" like ccast the 3G is awesome:)"
    },
    {
      "messageId":10,
      "authorId":1,
      "inResponseTo":12,
      "senderLocation":[42.5,70.01],
      "message":" can't stand product-w the touch-screen is terrible"
    },
    {
      "messageId":11,
      "authorId":1,
      "inResponseTo":1,
      "senderLocation":[38.97,77.49],
      "message":" can't stand acast its plan is terrible"
    } ]

## <a id="Select_clauses">SELECT Clause</a>
The SQL++ `SELECT` clause always returns a collection value as its result (even if the result is empty or a singleton).

### <a id="Select_element">Select Element/Value/Raw</a>
The `SELECT VALUE` clause in SQL++ returns an array or multiset that contains the results of evaluating the `VALUE`
expression, with one evaluation being performed per "binding tuple" (i.e., per `FROM` clause item) satisfying
the statement's selection criteria.
For historical reasons SQL++ also allows the keywords `ELEMENT` or `RAW` to be used in place of `VALUE`
(not recommended).

If there is no FROM clause, the expression after `VALUE` is evaluated once with no binding tuples
(except those inherited from an outer environment).

##### Example

    SELECT VALUE 1;

This query returns:

    [
      1
    ]

The following example shows a query that selects one user from the GleambookUsers collection.

##### Example

    SELECT VALUE user
    FROM GleambookUsers user
    WHERE user.id = 1;

This query returns:

    [{
        "userSince": "2012-08-20T10:10:00.000Z",
        "friendIds": [
            2,
            3,
            6,
            10
        ],
        "gender": "F",
        "name": "MargaritaStoddard",
        "nickname": "Mags",
        "alias": "Margarita",
        "id": 1,
        "employment": [
            {
                "organizationName": "Codetechno",
                "start-date": "2006-08-06"
            },
            {
                "end-date": "2010-01-26",
                "organizationName": "geomedia",
                "start-date": "2010-06-17"
            }
        ]
    } ]

### <a id="SQL_select">SQL-style SELECT</a>
In SQL++, the traditional SQL-style `SELECT` syntax is also supported.
This syntax can also be reformulated in a `SELECT VALUE` based manner in SQL++.
(E.g., `SELECT expA AS fldA, expB AS fldB` is syntactic sugar for `SELECT VALUE { 'fldA': expA, 'fldB': expB }`.)
Unlike in SQL, the result of an SQL++ query does not preserve the order of expressions in the `SELECT` clause.

##### Example
    SELECT user.alias user_alias, user.name user_name
    FROM GleambookUsers user
    WHERE user.id = 1;

Returns:

    [ {
        "user_name": "MargaritaStoddard",
        "user_alias": "Margarita"
    } ]

### <a id="Select_star">SELECT *</a>
In SQL++, `SELECT *` returns a object with a nested field for each input tuple.
Each field has as its field name the name of a binding variable generated by either the `FROM` clause or `GROUP BY`
clause in the current enclosing `SELECT` statement, and its field value is the value of that binding variable.

Note that the result of `SELECT *` is different from the result of query that selects all the fields of an object.

##### Example

    SELECT *
    FROM GleambookUsers user;

Since `user` is the only binding variable generated in the `FROM` clause, this query returns:

    [ {
        "user": {
            "userSince": "2012-08-20T10:10:00.000Z",
            "friendIds": [
                2,
                3,
                6,
                10
            ],
            "gender": "F",
            "name": "MargaritaStoddard",
            "nickname": "Mags",
            "alias": "Margarita",
            "id": 1,
            "employment": [
                {
                    "organizationName": "Codetechno",
                    "start-date": "2006-08-06"
                },
                {
                    "end-date": "2010-01-26",
                    "organizationName": "geomedia",
                    "start-date": "2010-06-17"
                }
            ]
        }
    }, {
        "user": {
            "userSince": "2011-01-22T10:10:00.000Z",
            "friendIds": [
                1,
                4
            ],
            "name": "IsbelDull",
            "nickname": "Izzy",
            "alias": "Isbel",
            "id": 2,
            "employment": [
                {
                    "organizationName": "Hexviafind",
                    "startDate": "2010-04-27"
                }
            ]
        }
    }, {
        "user": {
            "userSince": "2012-07-10T10:10:00.000Z",
            "friendIds": [
                1,
                5,
                8,
                9
            ],
            "name": "EmoryUnk",
            "alias": "Emory",
            "id": 3,
            "employment": [
                {
                    "organizationName": "geomedia",
                    "endDate": "2010-01-26",
                    "startDate": "2010-06-17"
                }
            ]
        }
    } ]


##### Example

    SELECT *
    FROM GleambookUsers u, GleambookMessages m
    WHERE m.authorId = u.id and u.id = 2;

This query does an inner join that we will discuss in [multiple from terms](#Multiple_from_terms).
Since both `u` and `m` are binding variables generated in the `FROM` clause, this query returns:

    [ {
        "u": {
            "userSince": "2011-01-22T10:10:00",
            "friendIds": [
                1,
                4
            ],
            "name": "IsbelDull",
            "nickname": "Izzy",
            "alias": "Isbel",
            "id": 2,
            "employment": [
                {
                    "organizationName": "Hexviafind",
                    "startDate": "2010-04-27"
                }
            ]
        },
        "m": {
            "senderLocation": [
                31.5,
                75.56
            ],
            "inResponseTo": 1,
            "messageId": 6,
            "authorId": 2,
            "message": " like product-z its platform is mind-blowing"
        }
    }, {
        "u": {
            "userSince": "2011-01-22T10:10:00",
            "friendIds": [
                1,
                4
            ],
            "name": "IsbelDull",
            "nickname": "Izzy",
            "alias": "Isbel",
            "id": 2,
            "employment": [
                {
                    "organizationName": "Hexviafind",
                    "startDate": "2010-04-27"
                }
            ]
        },
        "m": {
            "senderLocation": [
                48.09,
                81.01
            ],
            "inResponseTo": 4,
            "messageId": 3,
            "authorId": 2,
            "message": " like product-y the plan is amazing"
        }
    } ]


### <a id="Select_distinct">SELECT DISTINCT</a>
SQL++'s `DISTINCT` keyword is used to eliminate duplicate items in results. The following example shows how it works.

##### Example

    SELECT DISTINCT * FROM [1, 2, 2, 3] AS foo;

This query returns:

    [ {
        "foo": 1
    }, {
        "foo": 2
    }, {
        "foo": 3
    } ]

##### Example

    SELECT DISTINCT VALUE foo FROM [1, 2, 2, 3] AS foo;

This version of the query returns:

    [ 1
    , 2
    , 3
     ]

### <a id="Unnamed_projections">Unnamed Projections</a>
Similar to standard SQL, SQL++ supports unnamed projections (a.k.a, unnamed `SELECT` clause items), for which names are generated.
Name generation has three cases:

  * If a projection expression is a variable reference expression, its generated name is the name of the variable.
  * If a projection expression is a field access expression, its generated name is the last identifier in the expression.
  * For all other cases, the query processor will generate a unique name.

##### Example

    SELECT substr(user.name, 10), user.alias
    FROM GleambookUsers user
    WHERE user.id = 1;

This query outputs:

    [ {
        "alias": "Margarita",
        "$1": "Stoddard"
    } ]

In the result, `$1` is the generated name for `substr(user.name, 1)`, while `alias` is the generated name for `user.alias`.

### <a id="Abbreviated_field_access_expressions">Abbreviated Field Access Expressions</a>
As in standard SQL, SQL++ field access expressions can be abbreviated (not recommended) when there is no ambiguity. In the next example, the variable `user` is the only possible variable reference for fields `id`, `name` and `alias` and thus could be omitted in the query.

##### Example

    SELECT substr(name, 10) AS lname, alias
    FROM GleambookUsers user
    WHERE id = 1;

Outputs:

    [ {
        "lname": "Stoddard",
        "alias": "Margarita"
    } ]

## <a id="Unnest_clauses">UNNEST Clause</a>
For each of its input tuples, the `UNNEST` clause flattens a collection-valued expression into individual items, producing multiple tuples, each of which is one of the expression's original input tuples augmented with a flattened item from its collection.

### <a id="Inner_unnests">Inner UNNEST</a>
The following example is a query that retrieves the names of the organizations that a selected user has worked for. It uses the `UNNEST` clause to unnest the nested collection `employment` in the user's object.

##### Example

    SELECT u.id AS userId, e.organizationName AS orgName
    FROM GleambookUsers u
    UNNEST u.employment e
    WHERE u.id = 1;

This query returns:

    [ {
        "orgName": "Codetechno",
        "userId": 1
    }, {
        "orgName": "geomedia",
        "userId": 1
    } ]

Note that `UNNEST` has SQL's inner join semantics --- that is, if a user has no employment history, no tuple corresponding to that user will be emitted in the result.

### <a id="Left_outer_unnests">Left Outer UNNEST</a>
As an alternative, the `LEFT OUTER UNNEST` clause offers SQL's left outer join semantics. For example, no collection-valued field named `hobbies` exists in the object for the user whose id is 1, but the following query's result still includes user 1.

##### Example

    SELECT u.id AS userId, h.hobbyName AS hobby
    FROM GleambookUsers u
    LEFT OUTER UNNEST u.hobbies h
    WHERE u.id = 1;

Returns:

    [ {
        "userId": 1
    } ]

Note that if `u.hobbies` is an empty collection or leads to a `MISSING` (as above) or `NULL` value for a given input tuple, there is no corresponding binding value for variable `h` for an input tuple. A `MISSING` value will be generated for `h` so that the input tuple can still be propagated.

### <a id="Expressing_joins_using_unnests">Expressing Joins Using UNNEST</a>
The SQL++ `UNNEST` clause is similar to SQL's `JOIN` clause except that it allows its right argument to be correlated to its left argument, as in the examples above --- i.e., think "correlated cross-product".
The next example shows this via a query that joins two data sets, GleambookUsers and GleambookMessages, returning user/message pairs. The results contain one object per pair, with result objects containing the user's name and an entire message. The query can be thought of as saying "for each Gleambook user, unnest the `GleambookMessages` collection and filter the output with the condition `message.authorId = user.id`".

##### Example

    SELECT u.name AS uname, m.message AS message
    FROM GleambookUsers u
    UNNEST GleambookMessages m
    WHERE m.authorId = u.id;

This returns:

    [ {
        "uname": "MargaritaStoddard",
        "message": " can't stand acast its plan is terrible"
    }, {
        "uname": "MargaritaStoddard",
        "message": " dislike x-phone its touch-screen is horrible"
    }, {
        "uname": "MargaritaStoddard",
        "message": " can't stand acast the network is horrible:("
    }, {
        "uname": "MargaritaStoddard",
        "message": " like ccast the 3G is awesome:)"
    }, {
        "uname": "MargaritaStoddard",
        "message": " can't stand product-w the touch-screen is terrible"
    }, {
        "uname": "IsbelDull",
        "message": " like product-z its platform is mind-blowing"
    }, {
        "uname": "IsbelDull",
        "message": " like product-y the plan is amazing"
    } ]

Similarly, the above query can also be expressed as the `UNNEST`ing of a correlated SQL++ subquery:

##### Example

    SELECT u.name AS uname, m.message AS message
    FROM GleambookUsers u
    UNNEST (
        SELECT VALUE msg
        FROM GleambookMessages msg
        WHERE msg.authorId = u.id
    ) AS m;

## <a id="From_clauses">FROM clauses</a>
A `FROM` clause is used for enumerating (i.e., conceptually iterating over) the contents of collections, as in SQL.

### <a id="Binding_expressions">Binding expressions</a>
In SQL++, in addition to stored collections, a `FROM` clause can iterate over any intermediate collection returned by a valid SQL++ expression.
In the tuple stream generated by a `FROM` clause, the ordering of the input tuples are not guaranteed to be preserved.

##### Example

    SELECT VALUE foo
    FROM [1, 2, 2, 3] AS foo
    WHERE foo > 2;

Returns:

    [
      3
    ]

### <a id="Multiple_from_terms">Multiple FROM Terms</a>
SQL++ permits correlations among `FROM` terms. Specifically, a `FROM` binding expression can refer to variables defined to its left in the given `FROM` clause. Thus, the first unnesting example above could also be expressed as follows:

##### Example

    SELECT u.id AS userId, e.organizationName AS orgName
    FROM GleambookUsers u, u.employment e
    WHERE u.id = 1;


### <a id="Expressing_joins_using_from_terms">Expressing Joins Using FROM Terms</a>
Similarly, the join intentions of the other `UNNEST`-based join examples above could be expressed as:

##### Example

    SELECT u.name AS uname, m.message AS message
    FROM GleambookUsers u, GleambookMessages m
    WHERE m.authorId = u.id;

##### Example

    SELECT u.name AS uname, m.message AS message
    FROM GleambookUsers u,
      (
        SELECT VALUE msg
        FROM GleambookMessages msg
        WHERE msg.authorId = u.id
      ) AS m;

Note that the first alternative is one of the SQL-92 approaches to expressing a join.

### <a id="Implicit_binding_variables">Implicit Binding Variables</a>

Similar to standard SQL, SQL++ supports implicit `FROM` binding variables (i.e., aliases), for which a binding variable is generated. SQL++ variable generation falls into three cases:

  * If the binding expression is a variable reference expression, the generated variable's name will be the name of the referenced variable itself.
  * If the binding expression is a field access expression (or a fully qualified name for a dataset), the generated
    variable's name will be the last identifier (or the dataset name) in the expression.
  * For all other cases, a compilation error will be raised.

The next two examples show queries that do not provide binding variables in their `FROM` clauses.

##### Example

    SELECT GleambookUsers.name, GleambookMessages.message
    FROM GleambookUsers, GleambookMessages
    WHERE GleambookMessages.authorId = GleambookUsers.id;

Returns:

    [ {
        "name": "MargaritaStoddard",
        "message": " like ccast the 3G is awesome:)"
    }, {
        "name": "MargaritaStoddard",
        "message": " can't stand product-w the touch-screen is terrible"
    }, {
        "name": "MargaritaStoddard",
        "message": " can't stand acast its plan is terrible"
    }, {
        "name": "MargaritaStoddard",
        "message": " dislike x-phone its touch-screen is horrible"
    }, {
        "name": "MargaritaStoddard",
        "message": " can't stand acast the network is horrible:("
    }, {
        "name": "IsbelDull",
        "message": " like product-y the plan is amazing"
    }, {
        "name": "IsbelDull",
        "message": " like product-z its platform is mind-blowing"
    } ]

##### Example

    SELECT GleambookUsers.name, GleambookMessages.message
    FROM GleambookUsers,
      (
        SELECT VALUE GleambookMessages
        FROM GleambookMessages
        WHERE GleambookMessages.authorId = GleambookUsers.id
      );

Returns:

    Error: "Syntax error: Need an alias for the enclosed expression:\n(select element GleambookMessages\n    from GleambookMessages as GleambookMessages\n    where (GleambookMessages.authorId = GleambookUsers.id)\n )",
        "query_from_user": "use TinySocial;\n\nSELECT GleambookUsers.name, GleambookMessages.message\n    FROM GleambookUsers,\n      (\n        SELECT VALUE GleambookMessages\n        FROM GleambookMessages\n        WHERE GleambookMessages.authorId = GleambookUsers.id\n      );"

## <a id="Join_clauses">JOIN Clauses</a>
The join clause in SQL++ supports both inner joins and left outer joins from standard SQL.

### <a id="Inner_joins">Inner joins</a>
Using a `JOIN` clause, the inner join intent from the preceeding examples can also be expressed as follows:

##### Example

    SELECT u.name AS uname, m.message AS message
    FROM GleambookUsers u JOIN GleambookMessages m ON m.authorId = u.id;

### <a id="Left_outer_joins">Left Outer Joins</a>
SQL++ supports SQL's notion of left outer join. The following query is an example:

    SELECT u.name AS uname, m.message AS message
    FROM GleambookUsers u LEFT OUTER JOIN GleambookMessages m ON m.authorId = u.id;

Returns:

    [ {
        "uname": "MargaritaStoddard",
        "message": " like ccast the 3G is awesome:)"
    }, {
        "uname": "MargaritaStoddard",
        "message": " can't stand product-w the touch-screen is terrible"
    }, {
        "uname": "MargaritaStoddard",
        "message": " can't stand acast its plan is terrible"
    }, {
        "uname": "MargaritaStoddard",
        "message": " dislike x-phone its touch-screen is horrible"
    }, {
        "uname": "MargaritaStoddard",
        "message": " can't stand acast the network is horrible:("
    }, {
        "uname": "IsbelDull",
        "message": " like product-y the plan is amazing"
    }, {
        "uname": "IsbelDull",
        "message": " like product-z its platform is mind-blowing"
    }, {
        "uname": "EmoryUnk"
    } ]

For non-matching left-side tuples, SQL++ produces `MISSING` values for the right-side binding variables; that is why the last object in the above result doesn't have a `message` field. Note that this is slightly different from standard SQL, which instead would fill in `NULL` values for the right-side fields. The reason for this difference is that, for non-matches in its join results, SQL++ views fields from the right-side as being "not there" (a.k.a. `MISSING`) instead of as being "there but unknown" (i.e., `NULL`).

The left-outer join query can also be expressed using `LEFT OUTER UNNEST`:

    SELECT u.name AS uname, m.message AS message
    FROM GleambookUsers u
    LEFT OUTER UNNEST (
        SELECT VALUE message
        FROM GleambookMessages message
        WHERE message.authorId = u.id
      ) m;

In general, in SQL++, SQL-style join queries can also be expressed by `UNNEST` clauses and left outer join queries can be expressed by `LEFT OUTER UNNESTs`.

## <a id="Group_By_clauses">GROUP BY Clauses</a>
The SQL++ `GROUP BY` clause generalizes standard SQL's grouping and aggregation semantics, but it also retains backward compatibility with the standard (relational) SQL `GROUP BY` and aggregation features.

### <a id="Group_variables">Group variables</a>
In a `GROUP BY` clause, in addition to the binding variable(s) defined for the grouping key(s), SQL++ allows a user to define a *group variable* by using the clause's `GROUP AS` extension to denote the resulting group.
After grouping, then, the query's in-scope variables include the grouping key's binding variables as well as this group variable which will be bound to one collection value for each group. This per-group collection (i.e., multiset) value will be a set of nested objects in which each field of the object is the result of a renamed variable defined in parentheses following the group variable's name. The `GROUP AS` syntax is as follows:

    <GROUP> <AS> Variable ("(" Variable <AS> VariableReference ("," Variable <AS> VariableReference )* ")")?

##### Example

    SELECT *
    FROM GleambookMessages message
    GROUP BY message.authorId AS uid GROUP AS msgs(message AS msg);

This first example query returns:

    [ {
        "msgs": [
            {
                "msg": {
                    "senderLocation": [
                        38.97,
                        77.49
                    ],
                    "inResponseTo": 1,
                    "messageId": 11,
                    "authorId": 1,
                    "message": " can't stand acast its plan is terrible"
                }
            },
            {
                "msg": {
                    "senderLocation": [
                        41.66,
                        80.87
                    ],
                    "inResponseTo": 4,
                    "messageId": 2,
                    "authorId": 1,
                    "message": " dislike x-phone its touch-screen is horrible"
                }
            },
            {
                "msg": {
                    "senderLocation": [
                        37.73,
                        97.04
                    ],
                    "inResponseTo": 2,
                    "messageId": 4,
                    "authorId": 1,
                    "message": " can't stand acast the network is horrible:("
                }
            },
            {
                "msg": {
                    "senderLocation": [
                        40.33,
                        80.87
                    ],
                    "inResponseTo": 11,
                    "messageId": 8,
                    "authorId": 1,
                    "message": " like ccast the 3G is awesome:)"
                }
            },
            {
                "msg": {
                    "senderLocation": [
                        42.5,
                        70.01
                    ],
                    "inResponseTo": 12,
                    "messageId": 10,
                    "authorId": 1,
                    "message": " can't stand product-w the touch-screen is terrible"
                }
            }
        ],
        "uid": 1
    }, {
        "msgs": [
            {
                "msg": {
                    "senderLocation": [
                        31.5,
                        75.56
                    ],
                    "inResponseTo": 1,
                    "messageId": 6,
                    "authorId": 2,
                    "message": " like product-z its platform is mind-blowing"
                }
            },
            {
                "msg": {
                    "senderLocation": [
                        48.09,
                        81.01
                    ],
                    "inResponseTo": 4,
                    "messageId": 3,
                    "authorId": 2,
                    "message": " like product-y the plan is amazing"
                }
            }
        ],
        "uid": 2
    } ]

As we can see from the above query result, each group in the example query's output has an associated group
variable value called `msgs` that appears in the `SELECT *`'s result.
This variable contains a collection of objects associated with the group; each of the group's `message` values
appears in the `msg` field of the objects in the `msgs` collection.

The group variable in SQL++ makes more complex, composable, nested subqueries over a group possible, which is
important given the more complex data model of SQL++ (relative to SQL).
As a simple example of this, as we really just want the messages associated with each user, we might wish to avoid
the "extra wrapping" of each message as the `msg` field of a object.
(That wrapping is useful in more complex cases, but is essentially just in the way here.)
We can use a subquery in the `SELECT` clase to tunnel through the extra nesting and produce the desired result.

##### Example

    SELECT uid, (SELECT VALUE g.msg FROM g) AS msgs
    FROM GleambookMessages gbm
    GROUP BY gbm.authorId AS uid
    GROUP AS g(gbm as msg);

This variant of the example query returns:

       [ {
           "msgs": [
               {
                   "senderLocation": [
                       38.97,
                       77.49
                   ],
                   "inResponseTo": 1,
                   "messageId": 11,
                   "authorId": 1,
                   "message": " can't stand acast its plan is terrible"
               },
               {
                   "senderLocation": [
                       41.66,
                       80.87
                   ],
                   "inResponseTo": 4,
                   "messageId": 2,
                   "authorId": 1,
                   "message": " dislike x-phone its touch-screen is horrible"
               },
               {
                   "senderLocation": [
                       37.73,
                       97.04
                   ],
                   "inResponseTo": 2,
                   "messageId": 4,
                   "authorId": 1,
                   "message": " can't stand acast the network is horrible:("
               },
               {
                   "senderLocation": [
                       40.33,
                       80.87
                   ],
                   "inResponseTo": 11,
                   "messageId": 8,
                   "authorId": 1,
                   "message": " like ccast the 3G is awesome:)"
               },
               {
                   "senderLocation": [
                       42.5,
                       70.01
                   ],
                   "inResponseTo": 12,
                   "messageId": 10,
                   "authorId": 1,
                   "message": " can't stand product-w the touch-screen is terrible"
               }
           ],
           "uid": 1
       }, {
           "msgs": [
               {
                   "senderLocation": [
                       31.5,
                       75.56
                   ],
                   "inResponseTo": 1,
                   "messageId": 6,
                   "authorId": 2,
                   "message": " like product-z its platform is mind-blowing"
               },
               {
                   "senderLocation": [
                       48.09,
                       81.01
                   ],
                   "inResponseTo": 4,
                   "messageId": 3,
                   "authorId": 2,
                   "message": " like product-y the plan is amazing"
               }
           ],
           "uid": 2
       } ]

The next example shows a more interesting case involving the use of a subquery in the `SELECT` list.
Here the subquery further processes the groups.
There is no renaming in the declaration of the group variable `g` such that
`g` only has one field `gbm` which comes from the `FROM` clause.

##### Example

    SELECT uid,
           (SELECT VALUE g.gbm
            FROM g
            WHERE g.gbm.message LIKE '% like%'
            ORDER BY g.gbm.messageId
            LIMIT 2) AS msgs
    FROM GleambookMessages gbm
    GROUP BY gbm.authorId AS uid
    GROUP AS g;

This example query returns:

    [ {
        "msgs": [
            {
                "senderLocation": [
                    40.33,
                    80.87
                ],
                "inResponseTo": 11,
                "messageId": 8,
                "authorId": 1,
                "message": " like ccast the 3G is awesome:)"
            }
        ],
        "uid": 1
    }, {
        "msgs": [
            {
                "senderLocation": [
                    48.09,
                    81.01
                ],
                "inResponseTo": 4,
                "messageId": 3,
                "authorId": 2,
                "message": " like product-y the plan is amazing"
            },
            {
                "senderLocation": [
                    31.5,
                    75.56
                ],
                "inResponseTo": 1,
                "messageId": 6,
                "authorId": 2,
                "message": " like product-z its platform is mind-blowing"
            }
        ],
        "uid": 2
    } ]

### <a id="Implicit_group_key_variables">Implicit Grouping Key Variables</a>
In the SQL++ syntax, providing named binding variables for `GROUP BY` key expressions is optional.
If a grouping key is missing a user-provided binding variable, the underlying compiler will generate one.
Automatic grouping key variable naming falls into three cases in SQL++, much like the treatment of unnamed projections:

  * If the grouping key expression is a variable reference expression, the generated variable gets the same name as the referred variable;
  * If the grouping key expression is a field access expression, the generated variable gets the same name as the last identifier in the expression;
  * For all other cases, the compiler generates a unique variable (but the user query is unable to refer to this generated variable).

The next example illustrates a query that doesn't provide binding variables for its grouping key expressions.

##### Example

    SELECT authorId,
           (SELECT VALUE g.gbm
            FROM g
            WHERE g.gbm.message LIKE '% like%'
            ORDER BY g.gbm.messageId
            LIMIT 2) AS msgs
    FROM GleambookMessages gbm
    GROUP BY gbm.authorId
    GROUP AS g;

This query returns:

        [ {
        "msgs": [
            {
                "senderLocation": [
                    40.33,
                    80.87
                ],
                "inResponseTo": 11,
                "messageId": 8,
                "authorId": 1,
                "message": " like ccast the 3G is awesome:)"
            }
        ],
        "authorId": 1
    }, {
        "msgs": [
            {
                "senderLocation": [
                    48.09,
                    81.01
                ],
                "inResponseTo": 4,
                "messageId": 3,
                "authorId": 2,
                "message": " like product-y the plan is amazing"
            },
            {
                "senderLocation": [
                    31.5,
                    75.56
                ],
                "inResponseTo": 1,
                "messageId": 6,
                "authorId": 2,
                "message": " like product-z its platform is mind-blowing"
            }
        ],
        "authorId": 2
    } ]

Based on the three variable generation rules, the generated variable for the grouping key expression `message.authorId`
is `authorId` (which is how it is referred to in the example's `SELECT` clause).

### <a id="Implicit_group_variables">Implicit Group Variables</a>
The group variable itself is also optional in SQL++'s `GROUP BY` syntax.
If a user's query does not declare the name and structure of the group variable using `GROUP AS`,
the query compiler will generate a unique group variable whose fields include all of the binding
variables defined in the `FROM` clause of the current enclosing `SELECT` statement.
In this case the user's query will not be able to refer to the generated group variable,
but is able to call SQL-92 aggregation functions as in SQL-92.


### <a id="Aggregation_functions">Aggregation Functions</a>
In the traditional SQL, which doesn't support nested data, grouping always also involves the use of aggregation
to compute properties of the groups (for example, the average number of messages per user rather than the actual set
of messages per user).
Each aggregation function in SQL++ takes a collection (for example, the group of messages) as its input and produces
a scalar value as its output.
These aggregation functions, being truly functional in nature (unlike in SQL), can be used anywhere in a
query where an expression is allowed.
The following table catalogs the SQL++ built-in aggregation functions and also indicates how each one handles
`NULL`/`MISSING` values in the input collection or a completely empty input collection:

| Function       | NULL         | MISSING      | Empty Collection |
|----------------|--------------|--------------|------------------|
| COLL_COUNT     | counted      | counted      | 0                |
| COLL_SUM       | returns NULL | returns NULL | returns NULL     |
| COLL_MAX       | returns NULL | returns NULL | returns NULL     |
| COLL_MIN       | returns NULL | returns NULL | returns NULL     |
| COLL_AVG       | returns NULL | returns NULL | returns NULL     |
| ARRAY_COUNT    | not counted  | not counted  | 0                |
| ARRAY_SUM      | ignores NULL | ignores NULL | returns NULL     |
| ARRAY_MAX      | ignores NULL | ignores NULL | returns NULL     |
| ARRAY_MIN      | ignores NULL | ignores NULL | returns NULL     |
| ARRAY_AVG      | ignores NULL | ignores NULL | returns NULL     |

Notice that SQL++ has twice as many functions listed above as there are aggregate functions in SQL-92.
This is because SQL++ offers two versions of each -- one that handles `UNKNOWN` values in a semantically
strict fashion, where unknown values in the input result in unknown values in the output -- and one that
handles them in the ad hoc "just ignore the unknown values" fashion that the SQL standard chose to adopt.

##### Example

    ARRAY_AVG(
        (
          SELECT VALUE ARRAY_COUNT(friendIds) FROM GleambookUsers
        )
    );

This example returns:

    3.3333333333333335

##### Example

    SELECT uid AS uid, ARRAY_COUNT(grp) AS msgCnt
    FROM GleambookMessages message
    GROUP BY message.authorId AS uid
    GROUP AS grp(message AS msg);

This query returns:

    [ {
        "uid": 1,
        "msgCnt": 5
    }, {
        "uid": 2,
        "msgCnt": 2
    } ]

Notice how the query forms groups where each group involves a message author and their messages.
(SQL cannot do this because the grouped intermediate result is non-1NF in nature.)
The query then uses the collection aggregate function ARRAY_COUNT to get the cardinality of each
group of messages.

### <a id="SQL-92_aggregation_functions">SQL-92 Aggregation Functions</a>
For compatibility with the traditional SQL aggregation functions, SQL++ also offers SQL-92's
aggregation function symbols (`COUNT`, `SUM`, `MAX`, `MIN`, and `AVG`) as supported syntactic sugar.
The SQL++ compiler rewrites queries that utilize these function symbols into SQL++ queries that only
use the SQL++ collection aggregate functions. The following example uses the SQL-92 syntax approach
to compute a result that is identical to that of the more explicit SQL++ example above:

##### Example

    SELECT uid, COUNT(*) AS msgCnt
    FROM GleambookMessages msg
    GROUP BY msg.authorId AS uid;

It is important to realize that `COUNT` is actually **not** a SQL++ built-in aggregation function.
Rather, the `COUNT` query above is using a special "sugared" function symbol that the SQL++ compiler
will rewrite as follows:

    SELECT uid AS uid, ARRAY_COUNT( (SELECT VALUE 1 FROM `$1` as g) ) AS msgCnt
    FROM GleambookMessages msg
    GROUP BY msg.authorId AS uid
    GROUP AS `$1`(msg AS msg);


The same sort of rewritings apply to the function symbols `SUM`, `MAX`, `MIN`, and `AVG`.
In contrast to the SQL++ collection aggregate functions, these special SQL-92 function symbols
can only be used in the same way they are in standard SQL (i.e., with the same restrictions).

### <a id="SQL-92_compliant_gby">SQL-92 Compliant GROUP BY Aggregations</a>
SQL++ provides full support for SQL-92 `GROUP BY` aggregation queries.
The following query is such an example:

##### Example

    SELECT msg.authorId, COUNT(*)
    FROM GleambookMessages msg
    GROUP BY msg.authorId;

This query outputs:

    [ {
        "authorId": 1,
        "$1": 5
    }, {
        "authorId": 2,
        "$1": 2
    } ]

In principle, a `msg` reference in the query's `SELECT` clause would be "sugarized" as a collection
(as described in [Implicit Group Variables](#Implicit_group_variables)).
However, since the SELECT expression `msg.authorId` is syntactically identical to a GROUP BY key expression,
it will be internally replaced by the generated group key variable.
The following is the equivalent rewritten query that will be generated by the compiler for the query above:

    SELECT authorId AS authorId, ARRAY_COUNT( (SELECT g.msg FROM `$1` AS g) )
    FROM GleambookMessages msg
    GROUP BY msg.authorId AS authorId
    GROUP AS `$1`(msg AS msg);

### <a id="Column_aliases">Column Aliases</a>
SQL++ also allows column aliases to be used as `GROUP BY` keys or `ORDER BY` keys.

##### Example

    SELECT msg.authorId AS aid, COUNT(*)
    FROM GleambookMessages msg
    GROUP BY aid;

This query returns:

    [ {
        "$1": 5,
        "aid": 1
    }, {
        "$1": 2,
        "aid": 2
    } ]

## <a id="Where_having_clauses">WHERE Clauses and HAVING Clauses</a>
Both `WHERE` clauses and `HAVING` clauses are used to filter input data based on a condition expression.
Only tuples for which the condition expression evaluates to `TRUE` are propagated.
Note that if the condition expression evaluates to `NULL` or `MISSING` the input tuple will be disgarded.

## <a id="Order_By_clauses">ORDER BY Clauses</a>
The `ORDER BY` clause is used to globally sort data in either ascending order (i.e., `ASC`) or descending order (i.e., `DESC`).
During ordering, `MISSING` and `NULL` are treated as being smaller than any other value if they are encountered
in the ordering key(s). `MISSING` is treated as smaller than `NULL` if both occur in the data being sorted.
The following example returns all `GleambookUsers` in descending order by their number of friends.

##### Example

      SELECT VALUE user
      FROM GleambookUsers AS user
      ORDER BY ARRAY_COUNT(user.friendIds) DESC;

This query returns:

      [ {
          "userSince": "2012-08-20T10:10:00.000Z",
          "friendIds": [
              2,
              3,
              6,
              10
          ],
          "gender": "F",
          "name": "MargaritaStoddard",
          "nickname": "Mags",
          "alias": "Margarita",
          "id": 1,
          "employment": [
              {
                  "organizationName": "Codetechno",
                  "start-date": "2006-08-06"
              },
              {
                  "end-date": "2010-01-26",
                  "organizationName": "geomedia",
                  "start-date": "2010-06-17"
              }
          ]
      }, {
          "userSince": "2012-07-10T10:10:00.000Z",
          "friendIds": [
              1,
              5,
              8,
              9
          ],
          "name": "EmoryUnk",
          "alias": "Emory",
          "id": 3,
          "employment": [
              {
                  "organizationName": "geomedia",
                  "endDate": "2010-01-26",
                  "startDate": "2010-06-17"
              }
          ]
      }, {
          "userSince": "2011-01-22T10:10:00.000Z",
          "friendIds": [
              1,
              4
          ],
          "name": "IsbelDull",
          "nickname": "Izzy",
          "alias": "Isbel",
          "id": 2,
          "employment": [
              {
                  "organizationName": "Hexviafind",
                  "startDate": "2010-04-27"
              }
          ]
      } ]

## <a id="Limit_clauses">LIMIT Clauses</a>
The `LIMIT` clause is used to limit the result set to a specified constant size.
The use of the `LIMIT` clause is illustrated in the next example.

##### Example

      SELECT VALUE user
      FROM GleambookUsers AS user
      ORDER BY len(user.friendIds) DESC
      LIMIT 1;

This query returns:

      [ {
          "userSince": "2012-08-20T10:10:00.000Z",
          "friendIds": [
              2,
              3,
              6,
              10
          ],
          "gender": "F",
          "name": "MargaritaStoddard",
          "nickname": "Mags",
          "alias": "Margarita",
          "id": 1,
          "employment": [
              {
                  "organizationName": "Codetechno",
                  "start-date": "2006-08-06"
              },
              {
                  "end-date": "2010-01-26",
                  "organizationName": "geomedia",
                  "start-date": "2010-06-17"
              }
          ]
      } ]

## <a id="With_clauses">WITH Clauses</a>
As in standard SQL, `WITH` clauses are available to improve the modularity of a query.
The next query shows an example.

##### Example

    WITH avgFriendCount AS (
      SELECT VALUE AVG(ARRAY_COUNT(user.friendIds))
      FROM GleambookUsers AS user
    )[0]
    SELECT VALUE user
    FROM GleambookUsers user
    WHERE ARRAY_COUNT(user.friendIds) > avgFriendCount;

This query returns:

    [ {
        "userSince": "2012-08-20T10:10:00.000Z",
        "friendIds": [
            2,
            3,
            6,
            10
        ],
        "gender": "F",
        "name": "MargaritaStoddard",
        "nickname": "Mags",
        "alias": "Margarita",
        "id": 1,
        "employment": [
            {
                "organizationName": "Codetechno",
                "start-date": "2006-08-06"
            },
            {
                "end-date": "2010-01-26",
                "organizationName": "geomedia",
                "start-date": "2010-06-17"
            }
        ]
    }, {
        "userSince": "2012-07-10T10:10:00.000Z",
        "friendIds": [
            1,
            5,
            8,
            9
        ],
        "name": "EmoryUnk",
        "alias": "Emory",
        "id": 3,
        "employment": [
            {
                "organizationName": "geomedia",
                "endDate": "2010-01-26",
                "startDate": "2010-06-17"
            }
        ]
    } ]

The query is equivalent to the following, more complex, inlined form of the query:

    SELECT *
    FROM GleambookUsers user
    WHERE ARRAY_COUNT(user.friendIds) >
        ( SELECT VALUE AVG(ARRAY_COUNT(user.friendIds))
          FROM GleambookUsers AS user
        ) [0];

WITH can be particularly useful when a value needs to be used several times in a query.

Before proceeding further, notice that both  the WITH query and its equivalent inlined variant
include the syntax "[0]" -- this is due to a noteworthy difference between SQL++ and SQL-92.
In SQL-92, whenever a scalar value is expected and it is being produced by a query expression,
the SQL-92 query processor will evaluate the expression, check that there is only one row and column
in the result at runtime, and then coerce the one-row/one-column tabular result into a scalar value.
SQL++, being designed to deal with nested data and schema-less data, does not (and should not) do this.
Collection-valued data is perfectly legal in most SQL++ contexts, and its data is schema-less,
so a query processor rarely knows exactly what to expect where and such automatic conversion is often
not desirable. Thus, in the queries above, the use of "[0]" extracts the first (i.e., 0th) element of
an array-valued query expression's result; this is needed above, even though the result is an array of one
element, to extract the only element in the singleton array and obtain the desired scalar for the comparison.

## <a id="Let_clauses">LET Clauses</a>
Similar to `WITH` clauses, `LET` clauses can be useful when a (complex) expression is used several times within a query, allowing it to be written once to make the query more concise. The next query shows an example.

##### Example

    SELECT u.name AS uname, messages AS messages
    FROM GleambookUsers u
    LET messages = (SELECT VALUE m
                    FROM GleambookMessages m
                    WHERE m.authorId = u.id)
    WHERE EXISTS messages;

This query lists `GleambookUsers` that have posted `GleambookMessages` and shows all authored messages for each listed user. It returns:

    [ {
        "uname": "MargaritaStoddard",
        "messages": [
            {
                "senderLocation": [
                    38.97,
                    77.49
                ],
                "inResponseTo": 1,
                "messageId": 11,
                "authorId": 1,
                "message": " can't stand acast its plan is terrible"
            },
            {
                "senderLocation": [
                    41.66,
                    80.87
                ],
                "inResponseTo": 4,
                "messageId": 2,
                "authorId": 1,
                "message": " dislike x-phone its touch-screen is horrible"
            },
            {
                "senderLocation": [
                    37.73,
                    97.04
                ],
                "inResponseTo": 2,
                "messageId": 4,
                "authorId": 1,
                "message": " can't stand acast the network is horrible:("
            },
            {
                "senderLocation": [
                    40.33,
                    80.87
                ],
                "inResponseTo": 11,
                "messageId": 8,
                "authorId": 1,
                "message": " like ccast the 3G is awesome:)"
            },
            {
                "senderLocation": [
                    42.5,
                    70.01
                ],
                "inResponseTo": 12,
                "messageId": 10,
                "authorId": 1,
                "message": " can't stand product-w the touch-screen is terrible"
            }
        ]
    }, {
        "uname": "IsbelDull",
        "messages": [
            {
                "senderLocation": [
                    31.5,
                    75.56
                ],
                "inResponseTo": 1,
                "messageId": 6,
                "authorId": 2,
                "message": " like product-z its platform is mind-blowing"
            },
            {
                "senderLocation": [
                    48.09,
                    81.01
                ],
                "inResponseTo": 4,
                "messageId": 3,
                "authorId": 2,
                "message": " like product-y the plan is amazing"
            }
        ]
    } ]

This query is equivalent to the following query that does not use the `LET` clause:

    SELECT u.name AS uname, ( SELECT VALUE m
                              FROM GleambookMessages m
                              WHERE m.authorId = u.id
                            ) AS messages
    FROM GleambookUsers u
    WHERE EXISTS ( SELECT VALUE m
                   FROM GleambookMessages m
                   WHERE m.authorId = u.id
                 );

## <a id="Union_all">UNION ALL</a>
UNION ALL can be used to combine two input arrays or multisets into one. As in SQL, there is no ordering guarantee
on the contents of the output stream.
However, unlike SQL, SQL++ does not constrain what the data looks like on the input streams; in particular,
it allows heterogenity on the input and output streams.
A type error will be raised if one of the inputs is not a collection.
The following odd but legal query is an example:

##### Example

    SELECT u.name AS uname
    FROM GleambookUsers u
    WHERE u.id = 2
      UNION ALL
    SELECT VALUE m.message
    FROM GleambookMessages m
    WHERE authorId=2;

This query returns:

    [
      " like product-z its platform is mind-blowing"
      , {
        "uname": "IsbelDull"
    }, " like product-y the plan is amazing"
     ]

## <a id="Subqueries">Subqueries</a>
In SQL++, an arbitrary subquery can appear anywhere that an expression can appear.
Unlike SQL-92, as was just alluded to, the subqueries in a SELECT list or a boolean predicate need
not return singleton, single-column relations.
Instead, they may return arbitrary collections.
For example, the following query is a variant of the prior group-by query examples;
it retrieves an array of up to two "dislike" messages per user.

##### Example

    SELECT uid,
           (SELECT VALUE m.msg
            FROM msgs m
            WHERE m.msg.message LIKE '%dislike%'
            ORDER BY m.msg.messageId
            LIMIT 2) AS msgs
    FROM GleambookMessages message
    GROUP BY message.authorId AS uid GROUP AS msgs(message AS msg);

For our sample data set, this query returns:

    [ {
        "msgs": [
            {
                "senderLocation": [
                    41.66,
                    80.87
                ],
                "inResponseTo": 4,
                "messageId": 2,
                "authorId": 1,
                "message": " dislike x-phone its touch-screen is horrible"
            }
        ],
        "uid": 1
    }, {
        "msgs": [

        ],
        "uid": 2
    } ]

Note that a subquery, like a top-level `SELECT` statment, always returns a collection -- regardless of where
within a query the subquery occurs -- and again, its result is never automatically cast into a scalar.

## <a id="Vs_SQL-92">SQL++ vs. SQL-92</a>
SQL++ offers the following additional features beyond SQL-92 (hence the "++" in its name):

  * Fully composable and functional: A subquery can iterate over any intermediate collection and can appear anywhere in a query.
  * Schema-free: The query language does not assume the existence of a static schema for any data that it processes.
  * Correlated FROM terms: A right-side FROM term expression can refer to variables defined by FROM terms on its left.
  * Powerful GROUP BY: In addition to a set of aggregate functions as in standard SQL, the groups created by the `GROUP BY` clause are directly usable in nested queries and/or to obtain nested results.
  * Generalized SELECT clause: A SELECT clause can return any type of collection, while in SQL-92, a `SELECT` clause has to return a (homogeneous) collection of objects.


The following matrix is a quick "SQL-92 compatibility cheat sheet" for SQL++.

| Feature |  SQL++ | SQL-92 |  Why different?  |
|----------|--------|-------|------------------|
| SELECT * | Returns nested objects | Returns flattened concatenated objects | Nested collections are 1st class citizens |
| SELECT list | order not preserved | order preserved | Fields in a JSON object is not ordered |
| Subquery | Returns a collection  | The returned collection is cast into a scalar value if the subquery appears in a SELECT list or on one side of a comparison or as input to a function | Nested collections are 1st class citizens |
| LEFT OUTER JOIN |  Fills in `MISSING`(s) for non-matches  |   Fills in `NULL`(s) for non-matches    | "Absence" is more appropriate than "unknown" here.  |
| UNION ALL       | Allows heterogeneous inputs and output | Input streams must be UNION-compatible and output field names are drawn from the first input stream | Heterogenity and nested collections are common |
| IN constant_expr | The constant expression has to be an array or multiset, i.e., [..,..,...] | The constant collection can be represented as comma-separated items in a paren pair | Nested collections are 1st class citizens |
| String literal | Double quotes or single quotes | Single quotes only | Double quoted strings are pervasive |
| Delimited identifiers | Backticks | Double quotes | Double quoted strings are pervasive |

The following SQL-92 features are not implemented yet. However, SQL++ does not conflict those features:

  * CROSS JOIN, NATURAL JOIN, UNION JOIN
  * RIGHT and FULL OUTER JOIN
  * INTERSECT, EXCEPT, UNION with set semantics
  * CAST expression
  * NULLIF expression
  * COALESCE expression
  * ALL and SOME predicates for linking to subqueries
  * UNIQUE predicate (tests a collection for duplicates)
  * MATCH predicate (tests for referential integrity)
  * Row and Table constructors
  * DISTINCT aggregates
  * Preserved order for expressions in a SELECT list


