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

# The Asterix Query Language, Version 1.0

## <a id="toc">Table of Contents</a> ##

* [1. Introduction](#Introduction)
* [2. Expressions](#Expressions)
* [3. Statements](#Statements)

## <a id="Introduction">1. Introduction</a><font size="4"> <a href="#toc">[Back to TOC]</a></font>

This document is intended as a reference guide to the full syntax
and semantics of the Asterix Query Language (AQL), the language for talking to AsterixDB.
This guide covers both the data manipulation language (DML) aspects of AQL, including
its support for queries and data modification, as well as its data definition language
(DDL) aspects.
 New AsterixDB users are encouraged to read and work through the (friendlier) guide
"AsterixDB 101: An ADM and AQL Primer" before attempting to make use of this document.
In addition, readers are advised to read and understand the Asterix Data Model (ADM)
reference guide since a basic understanding of ADM concepts is a prerequisite to understanding AQL.
In what follows, we detail the features of the AQL language in a grammar-guided manner:
We list and briefly explain each of the productions in the AQL grammar, offering
examples for clarity in cases where doing so seems needed or helpful.

## <a id="Expressions">2. Expressions</a> <font size="4"><a href="#toc">[Back to TOC]</a></font>

    Query ::= Expression

An AQL query can be any legal AQL expression.

    Expression ::= ( OperatorExpr | IfThenElse | FLWOR | QuantifiedExpression )

AQL is a fully composable expression language.
Each AQL expression returns zero or more Asterix Data Model (ADM) instances.
There are four major kinds of expressions in AQL.
At the topmost level, an AQL expression can be an
OperatorExpr (similar to a mathematical expression),
an IfThenElse (to choose between two alternative values),
a FLWOR expression (the heart of AQL, pronounced "flower expression"),
or a QuantifiedExpression (which yields a boolean value).
Each will be detailed as we explore the full AQL grammar.

### Primary Expressions

    PrimaryExpr ::= Literal
                  | VariableRef
                  | ParenthesizedExpression
                  | FunctionCallExpr
                  | DatasetAccessExpression
                  | ListConstructor
                  | ObjectConstructor

The most basic building block for any AQL expression is the PrimaryExpr.
This can be a simple literal (constant) value,
a reference to a query variable that is in scope,
a parenthesized expression,
a function call,
an expression accessing the ADM contents of a dataset,
a newly constructed list of ADM instances,
or a newly constructed ADM object.

#### Literals

    Literal        ::= StringLiteral
                     | IntegerLiteral
                     | FloatLiteral
                     | DoubleLiteral
                     | "null"
                     | "true"
                     | "false"
    StringLiteral  ::= ("\"" (<ESCAPE_QUOT> | ~["\""])* "\"")
                     | ("\'" (<ESCAPE_APOS> | ~["\'"])* "\'")
    <ESCAPE_QUOT>  ::= "\\\""
    <ESCAPE_APOS>  ::= "\\\'"
    IntegerLiteral ::= <DIGITS>
    <DIGITS>       ::= ["0" - "9"]+
    FloatLiteral   ::= <DIGITS> ( "f" | "F" )
                     | <DIGITS> ( "." <DIGITS> ( "f" | "F" ) )?
                     | "." <DIGITS> ( "f" | "F" )
    DoubleLiteral  ::= <DIGITS>
                     | <DIGITS> ( "." <DIGITS> )?
                     | "." <DIGITS>

Literals (constants) in AQL can be strings, integers, floating point values,
double values, boolean constants, or the constant value null.
The null value in AQL has "unknown" or "missing" value semantics, similar to
(though not identical to) nulls in the relational query language SQL.

The following are some simple examples of AQL literals.
Since AQL is an expression language, each example is also a complete, legal AQL query (!).

##### Examples

    "a string"
    42

#### Variable References

    VariableRef ::= <VARIABLE>
    <VARIABLE>  ::= "$" <LETTER> (<LETTER> | <DIGIT> | "_")*
    <LETTER>    ::= ["A" - "Z", "a" - "z"]

A variable in AQL can be bound to any legal ADM value.
A variable reference refers to the value to which an in-scope variable is bound.
(E.g., a variable binding may originate from one of the for or let clauses of a
FLWOR expression or from an input parameter in the context of an AQL function body.)

##### Examples

    $tweet
    $id

#### Parenthesized Expressions

    ParenthesizedExpression ::= "(" Expression ")"

As in most languages, an expression may be parenthesized.

Since AQL is an expression language, the following example expression is
actually also a complete, legal AQL query whose result is the value 2.
(As such, you can have Big Fun explaining to your boss how AsterixDB and AQL can turn
your 1000-node shared-nothing Big Data cluster into a $5M calculator in its spare time.)

##### Example

    ( 1 + 1 )

#### Function Calls

    FunctionCallExpr ::= FunctionOrTypeName "(" ( Expression ( "," Expression )* )? ")"

Functions are included in AQL, like most languages, as a way to package useful
functionality or to componentize complicated or reusable AQL computations.
A function call is a legal AQL query expression that represents the ADM value
resulting from the evaluation of its body expression with the given parameter
bindings; the parameter value bindings can themselves be any AQL expressions.

The following example is a (built-in) function call expression whose value is 8.

##### Example

    string-length("a string")

#### Dataset Access

    DatasetAccessExpression ::= "dataset" ( ( Identifier ( "." Identifier )? )
                              | ( "(" Expression ")" ) )
    Identifier              ::= <IDENTIFIER> | StringLiteral
    <IDENTIFIER>            ::= <LETTER> (<LETTER> | <DIGIT> | <SPECIALCHARS>)*
    <SPECIALCHARS>          ::= ["$", "_", "-"]

Querying Big Data is the main point of AsterixDB and AQL.
Data in AsterixDB reside in datasets (collections of ADM objects),
each of which in turn resides in some namespace known as a dataverse (data universe).
Data access in a query expression is accomplished via a DatasetAccessExpression.
Dataset access expressions are most commonly used in FLWOR expressions, where variables
are bound to their contents.

Note that the Identifier that identifies a dataset (or any other Identifier in AQL) can also be a StringLiteral.
This is especially useful to avoid conficts with AQL keywords (e.g. "dataset", "null", or "type").

The following are three examples of legal dataset access expressions.
The first one accesses a dataset called Customers in the dataverse called SalesDV.
The second one accesses the Customers dataverse in whatever the current dataverse is.
The third one does the same thing as the second but uses a slightly older AQL syntax.

##### Examples

    dataset SalesDV.Customers
    dataset Customers
    dataset("Customers")

#### Constructors

    ListConstructor          ::= ( OrderedListConstructor | UnorderedListConstructor )
    OrderedListConstructor   ::= "[" ( Expression ( "," Expression )* )? "]"
    UnorderedListConstructor ::= "{{" ( Expression ( "," Expression )* )? "}}"
    ObjectConstructor        ::= "{" ( FieldBinding ( "," FieldBinding )* )? "}"
    FieldBinding             ::= Expression ":" Expression

A major feature of AQL is its ability to construct new ADM data instances.
This is accomplished using its constructors for each of the major ADM complex object structures,
namely lists (ordered or unordered) and objects.
Ordered lists are like JSON arrays, while unordered lists have bag (multiset) semantics.
Objects are built from attributes that are field-name/field-value pairs, again like JSON.
(See the AsterixDB Data Model document for more details on each.)

The following examples illustrate how to construct a new ordered list with 3 items,
a new unordered list with 4 items, and a new object with 2 fields, respectively.
List elements can be homogeneous (as in the first example), which is the common case,
or they may be heterogeneous (as in the second example).
The data values and field name values used to construct lists and objects in constructors are all simply AQL expressions.
Thus the list elements, field names, and field values used in constructors can be simple literals (as in these three examples)
or they can come from query variable references or even arbitrarily complex AQL expressions.

##### Examples

    [ "a", "b", "c" ]

    {{ 42, "forty-two", "AsterixDB!", 3.14f }}

    {
      "project name": "AsterixDB"
      "project members": {{ "vinayakb", "dtabass", "chenli" }}
    }

##### Note

When constructing nested objects there needs to be a space between the closing braces to avoid confusion with the `}}` token that ends an unordered list constructor:
`{ "a" : { "b" : "c" }}` will fail to parse while `{ "a" : { "b" : "c" } }` will work.

### Path Expressions

    ValueExpr ::= PrimaryExpr ( Field | Index )*
    Field     ::= "." Identifier
    Index     ::= "[" ( Expression | "?" ) "]"

Components of complex types in ADM are accessed via path expressions.
Path access can be applied to the result of an AQL expression that yields an instance of such a type, e.g., a object or list instance.
For objects, path access is based on field names.
For ordered lists, path access is based on (zero-based) array-style indexing.
AQL also supports an "I'm feeling lucky" style index accessor, [?], for selecting an arbitrary element from an ordered list.
Attempts to access non-existent fields or list elements produce a null (i.e., missing information) result as opposed to signaling a runtime error.

The following examples illustrate field access for a object, index-based element access for an ordered list, and also a composition thereof.

##### Examples

    ({"list": [ "a", "b", "c"]}).list

    (["a", "b", "c"])[2]

    ({ "list": [ "a", "b", "c"]}).list[2]

### Logical Expressions

    OperatorExpr ::= AndExpr ( "or" AndExpr )*
    AndExpr      ::= RelExpr ( "and" RelExpr )*

As in most languages, boolean expressions can be built up from smaller expressions by combining them with the logical connectives and/or.
Legal boolean values in AQL are true, false, and null.
(Nulls in AQL are treated much like SQL treats its unknown truth value in boolean expressions.)

The following is an example of a conjuctive range predicate in AQL.
It will yield true if $a is bound to 4, null if $a is bound to null, and false otherwise.

##### Example

    $a > 3 and $a < 5

### Comparison Expressions

    RelExpr ::= AddExpr ( ( "<" | ">" | "<=" | ">=" | "=" | "!=" | "~=" ) AddExpr )?

AQL has the usual list of suspects, plus one, for comparing pairs of atomic values.
The "plus one" is the last operator listed above, which is the "roughly equal" operator provided for similarity queries.
(See the separate document on [AsterixDB Similarity Queries](similarity.html) for more details on similarity matching.)

An example comparison expression (which yields the boolean value true) is shown below.

##### Example

    5 > 3

### Arithmetic Expressions

    AddExpr  ::= MultExpr ( ( "+" | "-" ) MultExpr )*
    MultExpr ::= UnaryExpr ( ( "*" | "/" | "div" | "%" | "mod" | "^" ) UnaryExpr )*
    UnaryExpr ::= ( ( "+" | "-" ) )? ValueExpr

AQL also supports the usual cast of characters for arithmetic expressions.
The example below evaluates to 25.

##### Example

    3 ^ 2 + 4 ^ 2

###  FLWOR Expression

    FLWOR         ::= ( ForClause | LetClause ) ( Clause )* ("return"|"select") Expression
    Clause         ::= ForClause | LetClause | WhereClause | OrderbyClause
                     | GroupClause | LimitClause | DistinctClause
    ForClause      ::= ("for"|"from") Variable ( "at" Variable )? "in" ( Expression )
    LetClause      ::= ("let"|"with") Variable ":=" Expression
    WhereClause    ::= "where" Expression
    OrderbyClause  ::= "order" "by" Expression ( ( "asc" ) | ( "desc" ) )?
                       ( "," Expression ( ( "asc" ) | ( "desc" ) )? )*
    GroupClause    ::= "group" "by" ( Variable ":=" )? Expression ( "," ( Variable ":=" )? Expression )*
                       ("with"|"keeping") VariableRef ( "," VariableRef )*
    LimitClause    ::= "limit" Expression ( "offset" Expression )?
    DistinctClause ::= "distinct" "by" Expression ( "," Expression )*
    Variable       ::= <VARIABLE>

The heart of AQL is the FLWOR (for-let-where-orderby-return) expression.
The roots of this expression were borrowed from the expression of the same name in XQuery.
A FLWOR expression starts with one or more clauses that establish variable bindings.
A `for` clause binds a variable incrementally to each element of its associated expression;
it includes an optional positional variable for counting/numbering the bindings.
By default no ordering is implied or assumed by a `for` clause.
A `let` clause binds a variable to the collection of elements computed by its associated expression.

Following the initial `for` or `let` clause(s), a FLWOR expression may contain an arbitrary sequence of other clauses.
The `where` clause in a FLWOR expression filters the preceding bindings via a boolean expression, much like a `where` clause does in a SQL query.
The `order by` clause in a FLWOR expression induces an ordering on the data.
The `group by` clause, discussed further below, forms groups based on its group by expressions,
optionally naming the expressions' values (which together form the grouping key for the expression).
The `with` subclause of a `group by` clause specifies the variable(s) whose values should be grouped based
on the grouping key(s); following the grouping clause, only the grouping key(s) and the variables named
in the with subclause remain in scope, and the named grouping variables now contain lists formed from their input values.
The `limit` clause caps the number of values returned, optionally starting its result count from a specified offset.
(Web applications can use this feature for doing pagination.)
The `distinct` clause is similar to the `group-by` clause, but it forms no groups; it serves only to eliminate duplicate values.
As indicated by the grammar, the clauses in an AQL query can appear in any order.
To interpret a query, one can think of data as flowing down through the query from the first clause to the `return` clause.

The following example shows a FLWOR expression that selects and returns one user from the dataset FacebookUsers.

##### Example

    for $user in dataset FacebookUsers
    where $user.id = 8
    return $user

The next example shows a FLWOR expression that joins two datasets, FacebookUsers and FacebookMessages,
returning user/message pairs.
The results contain one object per pair, with result objects containing the user's name and an entire message.

##### Example

    for $user in dataset FacebookUsers
    for $message in dataset FacebookMessages
    where $message.author-id = $user.id
    return
      {
        "uname": $user.name,
        "message": $message.message
      };

In the next example, a `let` clause is used to bind a variable to all of a user's FacebookMessages.
The query returns one object per user, with result objects containing the user's name and the set of all messages by that user.

##### Example

    for $user in dataset FacebookUsers
    let $messages :=
      for $message in dataset FacebookMessages
      where $message.author-id = $user.id
      return $message.message
    return
      {
        "uname": $user.name,
        "messages": $messages
      };

The following example returns all TwitterUsers ordered by their followers count (most followers first) and language.
When ordering `null` is treated as being smaller than any other value if `null`s are encountered in the ordering key(s).

##### Example

      for $user in dataset TwitterUsers
      order by $user.followers_count desc, $user.lang asc
      return $user

The next example illustrates the use of the `group by` clause in AQL.
After the `group by` clause in the query, only variables that are either in the `group by` list or in the `with` list are in scope.
The variables in the clause's `with` list will each contain a collection of items following the `group by` clause;
the collected items are the values that the source variable was bound to in the tuples that formed the group.
For grouping `null` is handled as a single value.

##### Example

      for $x in dataset FacebookMessages
      let $messages := $x.message
      group by $loc := $x.sender-location with $messages
      return
        {
          "location" : $loc,
          "message" : $messages
        }

The use of the `limit` clause is illustrated in the next example.

##### Example

      for $user in dataset TwitterUsers
      order by $user.followers_count desc
      limit 2
      return $user

The final example shows how AQL's `distinct by` clause works.
Each variable in scope before the distinct clause is also in scope after the `distinct by` clause.
This clause works similarly to `group by`, but for each variable that contains more than
one value after the `distinct by` clause, one value is picked nondeterministically.
(If the variable is in the `distinct by` list, then its value will be deterministic.)
Nulls are treated as a single value when they occur in a grouping field.

##### Example

      for $x in dataset FacebookMessages
      distinct by $x.sender-location
      return
        {
          "location" : $x.sender-location,
          "message" : $x.message
        }

In order to allow SQL fans to write queries in their favored ways,
AQL provides synonyms:  _from_ for _for_, _select_ for _return_,  _with_ for _let_, and
_keeping_ for _with_ in the group by clause. The following query
is such an example.

##### Example
      from $x in dataset FacebookMessages
      with $messages := $x.message
      group by $loc := $x.sender-location keeping $messages
      select
        {
          "location" : $loc,
          "message" : $messages
        }

### Conditional Expression

    IfThenElse ::= "if" "(" Expression ")" "then" Expression "else" Expression

A conditional expression is useful for choosing between two alternative values based on a
boolean condition.  If its first (`if`) expression is true, its second (`then`) expression's
value is returned, and otherwise its third (`else`) expression is returned.

The following example illustrates the form of a conditional expression.
##### Example

    if (2 < 3) then "yes" else "no"

### Quantified Expressions

    QuantifiedExpression ::= ( ( "some" ) | ( "every" ) ) Variable "in" Expression
                             ( "," Variable "in" Expression )* "satisfies" Expression

Quantified expressions are used for expressing existential or universal predicates involving the elements of a collection.

The following pair of examples illustrate the use of a quantified expression to test that every (or some) element in the set [1, 2, 3] of integers is less than three.
The first example yields `false` and second example yields `true`.

It is useful to note that if the set were instead the empty set, the first expression would yield `true`
("every" value in an empty set satisfies the condition) while the second expression would yield `false`
(since there isn't "some" value, as there are no values in the set, that satisfies the condition).

##### Examples

    every $x in [ 1, 2, 3 ] satisfies $x < 3
    some $x in [ 1, 2, 3 ] satisfies $x < 3

## <a id="Statements">3. Statements</a> <font size="4"><a href="#toc">[Back to TOC]</a></font>

    Statement ::= ( SingleStatement ( ";" )? )* <EOF>
    SingleStatement ::= DataverseDeclaration
                      | FunctionDeclaration
                      | CreateStatement
                      | DropStatement
                      | LoadStatement
                      | SetStatement
                      | InsertStatement
                      | DeleteStatement
                      | UpsertStatement
                      | Query

In addition to expresssions for queries, AQL supports a variety of statements for data
definition and manipulation purposes as well as controlling the context to be used in
evaluating AQL expressions. AQL supports object-level ACID transactions that begin and terminate implicitly for each object inserted, deleted, upserted, or searched while a given AQL statement is being executed.

This section details the statements supported in the AQL language.

### Declarations

    DataverseDeclaration ::= "use" "dataverse" Identifier

The world of data in an AsterixDB cluster is organized into data namespaces called dataverses.
To set the default dataverse for a series of statements, the use dataverse statement is provided.

As an example, the following statement sets the default dataverse to be TinySocial.

##### Example

    use dataverse TinySocial;

The set statement in AQL is used to control aspects of the expression evalation context for queries.

    SetStatement ::= "set" Identifier StringLiteral

As an example, the following set statements request that Jaccard similarity with a similarity threshold 0.6
be used for set similarity matching when the ~= operator is used in a query expression.

##### Example

    set simfunction "jaccard";
    set simthreshold "0.6f";

When writing a complex AQL query, it can sometimes be helpful to define one or more
auxilliary functions that each address a sub-piece of the overall query.
The declare function statement supports the creation of such helper functions.

    FunctionDeclaration  ::= "declare" "function" Identifier ParameterList "{" Expression "}"
    ParameterList        ::= "(" ( <VARIABLE> ( "," <VARIABLE> )* )? ")"

The following is a very simple example of a temporary AQL function definition.

##### Example

    declare function add($a, $b) {
      $a + $b
    };

### Lifecycle Management Statements

    CreateStatement ::= "create" ( DataverseSpecification
                                 | TypeSpecification
                                 | DatasetSpecification
                                 | IndexSpecification
                                 | FunctionSpecification )

    QualifiedName       ::= Identifier ( "." Identifier )?
    DoubleQualifiedName ::= Identifier "." Identifier ( "." Identifier )?

The create statement in AQL is used for creating persistent artifacts in the context of dataverses.
It can be used to create new dataverses, datatypes, datasets, indexes, and user-defined AQL functions.

#### Dataverses

    DataverseSpecification ::= "dataverse" Identifier IfNotExists ( "with format" StringLiteral )?

The create dataverse statement is used to create new dataverses.
To ease the authoring of reusable AQL scripts, its optional IfNotExists clause allows creation
to be requested either unconditionally or only if the the dataverse does not already exist.
If this clause is absent, an error will be returned if the specified dataverse already exists.
The `with format` clause is a placeholder for future functionality that can safely be ignored.

The following example creates a dataverse named TinySocial.

##### Example

    create dataverse TinySocial;

#### Types

    TypeSpecification    ::= "type" FunctionOrTypeName IfNotExists "as" TypeExpr
    FunctionOrTypeName   ::= QualifiedName
    IfNotExists          ::= ( "if not exists" )?
    TypeExpr             ::= ObjectTypeDef | TypeReference | OrderedListTypeDef | UnorderedListTypeDef
    ObjectTypeDef        ::= ( "closed" | "open" )? "{" ( ObjectField ( "," ObjectField )* )? "}"
    ObjectField          ::= Identifier ":" ( TypeExpr ) ( "?" )?
    NestedField          ::= Identifier ( "." Identifier )*
    IndexField           ::= NestedField ( ":" TypeReference )?
    TypeReference        ::= Identifier
    OrderedListTypeDef   ::= "[" ( TypeExpr ) "]"
    UnorderedListTypeDef ::= "{{" ( TypeExpr ) "}}"

The create type statement is used to create a new named ADM datatype.
This type can then be used to create datasets or utilized when defining one or more other ADM datatypes.
Much more information about the Asterix Data Model (ADM) is available in the [data model reference guide](datamodel.html) to ADM.
A new type can be a object type, a renaming of another type, an ordered list type, or an unordered list type.
A object type can be defined as being either open or closed.
Instances of a closed object type are not permitted to contain fields other than those specified in the create type statement.
Instances of an open object type may carry additional fields, and open is the default for a new type (if neither option is specified).

The following example creates a new ADM object type called FacebookUser type.
Since it is closed, its instances will contain only what is specified in the type definition.
The first four fields are traditional typed name/value pairs.
The friend-ids field is an unordered list of 32-bit integers.
The employment field is an ordered list of instances of another named object type, EmploymentType.

##### Example

    create type FacebookUserType as closed {
      "id" :         int32,
      "alias" :      string,
      "name" :       string,
      "user-since" : datetime,
      "friend-ids" : {{ int32 }},
      "employment" : [ EmploymentType ]
    }

The next example creates a new ADM object type called FbUserType. Note that the type of the id field is UUID. You need to use this field type if you want to have this field be an autogenerated-PK field. Refer to the Datasets section later for more details.

##### Example

    create type FbUserType as closed {
      "id" :         uuid,
      "alias" :      string,
      "name" :       string
    }

#### Datasets

    DatasetSpecification ::= "internal"? "dataset" QualifiedName "(" QualifiedName ")" IfNotExists
                             PrimaryKey ( "on" Identifier )? ( "hints" Properties )?
                             ( "using" "compaction" "policy" CompactionPolicy ( Configuration )? )?
                             ( "with filter on" Identifier )?
                           | "external" "dataset" QualifiedName "(" QualifiedName ")" IfNotExists
                             "using" AdapterName Configuration ( "hints" Properties )?
                             ( "using" "compaction" "policy" CompactionPolicy ( Configuration )? )?
    AdapterName          ::= Identifier
    Configuration        ::= "(" ( KeyValuePair ( "," KeyValuePair )* )? ")"
    KeyValuePair         ::= "(" StringLiteral "=" StringLiteral ")"
    Properties           ::= ( "(" Property ( "," Property )* ")" )?
    Property             ::= Identifier "=" ( StringLiteral | IntegerLiteral )
    FunctionSignature    ::= FunctionOrTypeName "@" IntegerLiteral
    PrimaryKey           ::= "primary" "key" NestedField ( "," NestedField )* ( "autogenerated ")?
    CompactionPolicy     ::= Identifier
    PrimaryKey           ::= "primary" "key" Identifier ( "," Identifier )* ( "autogenerated ")?

The create dataset statement is used to create a new dataset.
Datasets are named, unordered collections of ADM object instances; they
are where data lives persistently and are the targets for queries in AsterixDB.
Datasets are typed, and AsterixDB will ensure that their contents conform to their type definitions.
An Internal dataset (the default) is a dataset that is stored in and managed by AsterixDB.
It must have a specified unique primary key that can be used to partition data across nodes of an AsterixDB cluster.
The primary key is also used in secondary indexes to uniquely identify the indexed primary data objects. Random primary key (UUID) values can be auto-generated by declaring the field to be UUID and putting "autogenerated" after the "primary key" identifier. In this case, values for the auto-generated PK field should not be provided by the user since it will be auto-generated by AsterixDB.
Optionally, a filter can be created on a field to further optimize range queries with predicates on the filter's field.
(Refer to [Filter-Based LSM Index Acceleration](filters.html) for more information about filters.)

An External dataset is stored outside of AsterixDB (currently datasets in HDFS or on the local filesystem(s) of the cluster's nodes are supported).
External dataset support allows AQL queries to treat external data as though it were stored in AsterixDB,
making it possible to query "legacy" file data (e.g., Hive data) without having to physically import it into AsterixDB.
For an external dataset, an appropriate adapter must be selected to handle the nature of the desired external data.
(See the [guide to external data](externaldata.html) for more information on the available adapters.)

When creating a dataset, it is possible to choose a merge policy that controls
which of the underlaying LSM storage components to be merged.  Currently,
AsterixDB provides four different merge policies that can be
configured per dataset: no-merge, constant, prefix, and correlated-prefix. The
no-merge policy simply never merges disk components. While the constant policy merges disk components when the
number of components reaches some constant number k, which can be
configured by the user. The prefix policy relies on component sizes and the number of
components to decide which components to merge. Specifically, it works
by first trying to identify the smallest ordered (oldest to newest)
sequence of components such that the sequence does not contain a
single component that exceeds some threshold size M and that either
the sum of the component's sizes exceeds M or the number of
components in the sequence exceeds another threshold C. If such a
sequence of components exists, then each of the components in the
sequence are merged together to form a single component. Finally, the correlated-prefix is similar to the prefix policy but it
delegates the decision of merging the disk components of all the
indexes in a dataset to the primary index. When the policy decides
that the primary index needs to be merged (using the same decision
criteria as for the prefix policy), then it will issue successive
merge requests on behalf of all other indexes
associated with the same dataset. The default policy for
AsterixDB is the prefix policy except when there is a filter on a dataset, where the preferred policy for filters is the correlated-prefix.


The following example creates an internal dataset for storing FacefookUserType objects.
It specifies that their id field is their primary key.

##### Example
    create internal dataset FacebookUsers(FacebookUserType) primary key id;

The following example creates an internal dataset for storing FbUserType objects.
It specifies that their id field is their primary key. It also specifies that the id field is an auto-generated field, meaning that a randomly generated UUID value will be assigned to each object by the system. (A user should therefore not proivde a value for this field.) Note that the id field should be UUID.

##### Example
    create internal dataset FbMsgs(FbUserType) primary key id autogenerated;

The next example creates an external dataset for storing LineitemType objects.
The choice of the `hdfs` adapter means that its data will reside in HDFS.
The create statement provides parameters used by the hdfs adapter:
the URL and path needed to locate the data in HDFS and a description of the data format.

##### Example
    create external dataset Lineitem('LineitemType) using hdfs (
      ("hdfs"="hdfs://HOST:PORT"),
      ("path"="HDFS_PATH"),
      ("input-format"="text-input-format"),
      ("format"="delimited-text"),
      ("delimiter"="|"));

#### Indices

    IndexSpecification ::= "index" Identifier IfNotExists "on" QualifiedName
                           "(" ( IndexField ) ( "," IndexField )* ")" ( "type" IndexType )? ( "enforced" )?
    IndexType          ::= "btree"
                         | "rtree"
                         | "keyword"
                         | "ngram" "(" IntegerLiteral ")"
                         | "fulltext"

The create index statement creates a secondary index on one or more fields of a specified dataset.
Supported index types include `btree` for totally ordered datatypes,
`rtree` for spatial data, and `keyword`, `ngram`, and `fulltext` for textual (string) data.
An index can be created on a nested field (or fields) by providing a valid path expression as an index field identifier.
An index field is not required to be part of the datatype associated with a dataset if that datatype is declared as
open and the field's type is provided along with its type and the `enforced` keyword is specified in the end of index definition.
`Enforcing` an open field will introduce a check that will make sure that the actual type of an indexed
field (if the field exists in the object) always matches this specified (open) field type.

The following example creates a btree index called fbAuthorIdx on the author-id field of the FacebookMessages dataset.
This index can be useful for accelerating exact-match queries, range search queries, and joins involving the author-id field.

##### Example

    create index fbAuthorIdx on FacebookMessages(author-id) type btree;

The following example creates an open btree index called fbSendTimeIdx on the open send-time field of the
FacebookMessages dataset having datetime type.
This index can be useful for accelerating exact-match queries, range search queries, and joins involving the send-time field.

##### Example

    create index fbSendTimeIdx on FacebookMessages(send-time:datetime) type btree enforced;

The following example creates a btree index called twUserScrNameIdx on the screen-name field, which is a nested field
of the user field in the TweetMessages dataset.
This index can be useful for accelerating exact-match queries, range search queries, and joins involving the screen-name field.

##### Example

    create index twUserScrNameIdx on TweetMessages(user.screen-name) type btree;


The following example creates an rtree index called fbSenderLocIdx on the sender-location field of the FacebookMessages dataset.
This index can be useful for accelerating queries that use the
[`spatial-intersect` function](functions.html#spatial-intersect) in a predicate involving the
sender-location field.

##### Example

    create index fbSenderLocIndex on FacebookMessages(sender-location) type rtree;

The following example creates a 3-gram index called fbUserIdx on the name field of the FacebookUsers dataset.
This index can be used to accelerate some similarity or substring maching queries on the name field.
For details refer to the [document on similarity queries](similarity.html#NGram_Index).

##### Example

    create index fbUserIdx on FacebookUsers(name) type ngram(3);

The following example creates a keyword index called fbMessageIdx on the message field of the FacebookMessages dataset.
This keyword index can be used to optimize queries with token-based similarity predicates on the message field.
For details refer to the [document on similarity queries](similarity.html#Keyword_Index).

##### Example

    create index fbMessageIdx on FacebookMessages(message) type keyword;

The following example creates a full-text index called fbMessageIdx on the message field of the FacebookMessages dataset.
This full-text index can be used to optimize queries with full-text search predicates on the message field.
For details refer to the [document on full-text queries](fulltext.html#toc).

##### Example

    create index fbMessageIdx on FacebookMessages(message) type fulltext;


#### Functions

The create function statement creates a named function that can then be used and reused in AQL queries.
The body of a function can be any AQL expression involving the function's parameters.

    FunctionSpecification ::= "function" FunctionOrTypeName IfNotExists ParameterList "{" Expression "}"

The following is a very simple example of a create function statement.
It differs from the declare function example shown previously in that it results in a function that is
persistently registered by name in the specified dataverse.

##### Example

    create function add($a, $b) {
      $a + $b
    };

#### Removal

    DropStatement       ::= "drop" ( "dataverse" Identifier IfExists
                                   | "type" FunctionOrTypeName IfExists
                                   | "dataset" QualifiedName IfExists
                                   | "index" DoubleQualifiedName IfExists
                                   | "function" FunctionSignature IfExists )
    IfExists            ::= ( "if" "exists" )?

The drop statement in AQL is the inverse of the create statement.
It can be used to drop dataverses, datatypes, datasets, indexes, and functions.

The following examples illustrate uses of the drop statement.

##### Example

    drop dataset FacebookUsers if exists;

    drop index FacebookUsers.fbSenderLocIndex;

    drop type FacebookUserType;

    drop dataverse TinySocial;

    drop function add;

### Import/Export Statements

    LoadStatement  ::= "load" "dataset" QualifiedName "using" AdapterName Configuration ( "pre-sorted" )?

The load statement is used to initially populate a dataset via bulk loading of data from an external file.
An appropriate adapter must be selected to handle the nature of the desired external data.
The load statement accepts the same adapters and the same parameters as external datasets.
(See the [guide to external data](externaldata.html) for more information on the available adapters.)
If a dataset has an auto-generated primary key field, a file to be imported should not include that field in it.

The following example shows how to bulk load the FacebookUsers dataset from an external file containing
data that has been prepared in ADM format.

##### Example

    load dataset FacebookUsers using localfs
    (("path"="localhost:///Users/zuck/AsterixDB/load/fbu.adm"),("format"="adm"));

### Modification Statements

#### Insert

    InsertStatement ::= "insert" "into" "dataset" QualifiedName ( "as" Variable )? Query ( "returning" Query )?

The AQL insert statement is used to insert data into a dataset.
The data to be inserted comes from an AQL query expression.
The expression can be as simple as a constant expression, or in general it can be any legal AQL query.
Inserts in AsterixDB are processed transactionally, with the scope of each insert transaction
being the insertion of a single object plus its affiliated secondary index entries (if any).
If the query part of an insert returns a single object, then the insert statement itself will
be a single, atomic transaction.
If the query part returns multiple objects, then each object inserted will be handled independently
as a tranaction. If a dataset has an auto-generated primary key field, an insert statement should not include a value for that field in it. (The system will automatically extend the provided object with this additional field and a corresponding value.).
The optional "as Variable" provides a variable binding for the inserted objects, which can be used in the "returning" clause.
The optional "returning Query" allows users to run simple queries/functions on the objects returned by the insert.
This query cannot refer to any datasets.

The following example illustrates a query-based insertion.

##### Example

    insert into dataset UsersCopy as $inserted (for $user in dataset FacebookUsers return $user ) returning $inserted.screen-name

#### Delete

    DeleteStatement ::= "delete" Variable "from" "dataset" QualifiedName ( "where" Expression )?

The AQL delete statement is used to delete data from a target dataset.
The data to be deleted is identified by a boolean expression involving the variable bound to the
target dataset in the delete statement.
Deletes in AsterixDB are processed transactionally, with the scope of each delete transaction
being the deletion of a single object plus its affiliated secondary index entries (if any).
If the boolean expression for a delete identifies a single object, then the delete statement itself
will be a single, atomic transaction.
If the expression identifies multiple objects, then each object deleted will be handled independently
as a transaction.

The following example illustrates a single-object deletion.

##### Example

    delete $user from dataset FacebookUsers where $user.id = 8;

#### Upsert

    UpsertStatement ::= "upsert" "into" "dataset" QualifiedName Query

The AQL upsert statement is used to couple delete (if found) with insert data into a dataset.
The data to be upserted comes from an AQL query expression.
The expression can be as simple as a constant expression, or in general it can be any legal AQL query.
Upserts in AsterixDB are processed transactionally, with the scope of each upsert transaction
being the upsertion (deletion if found + insertion) of a single object plus its affiliated
secondary index entries (if any).
If the query part of an upsert returns a single object, then the upsert statement itself will
be a single, atomic transaction.
If the query part returns multiple objects, then each object upserted will be handled independently
as a tranaction.

The following example illustrates a query-based upsertion.

##### Example

    upsert into dataset Users (for $user in dataset FacebookUsers return $user)

We close this guide to AQL with one final example of a query expression.

##### Example

    for $praise in {{ "great", "brilliant", "awesome" }}
    return
       string-concat(["AsterixDB is ", $praise])


