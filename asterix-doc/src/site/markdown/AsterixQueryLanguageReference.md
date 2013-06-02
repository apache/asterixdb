# The Asterix Query Language, Version 1.0
## 1. Introduction

This document is intended to serve as a reference-style guide to the full syntax
and semantics of the Asterix Query Language (AQL), the language for talking to AsterixDB.
This guide covers both the data manipulation language (DML) aspects of AQL, including
its support for queries and data modification, as well as its data definition language
(DDL) aspects.
 New AsterixDB users are encouraged to read and work through the (friendlier) guide
"AsterixDB 101: An ADM and AQL Primer" before attempting to make use of this document.
In addition, readers are advised to read and understand the Asterix Data Model (ADM)
reference guide, as a basic understanding of ADM is a prerequisite to understanding AQL.  
In what follows, we detail the features of the AQL language in a grammar-guided manner:
We list and briefly explain each of the productions in the AQL grammar, offering brief
examples for clarity in cases where doing so seems needed.

## 2. Expressions

    Expression ::= ( OperatorExpr | IfThenElse | FLWOR | QuantifiedExpression )

AQL is a fully composable expression language.
Each AQL query is an expression that returns a collection
of zero or more Asterix Data Model (ADM) instances.
There are four major kinds of query expression in AQL.
At the top level, a query expression can be an
OperatorExpr (similar to a mathematical expression),
an IfThenElse (to choose between two alternative values),
a FLWOR expression (the heart of AQL, pronounced "flower expression"),
or a QuantifiedExpression (which yields a boolean value).

### Primary Expressions

    PrimaryExpr ::= Literal
                  | VariableRef
                  | ParenthesizedExpression
                  | FunctionCallExpr
                  | DatasetAccessExpression
                  | ListConstructor
                  | RecordConstructor

The most basic building block for any AQL expression is the PrimaryExpr.
This can be a simple literal (constant) value,
a reference to a query variable that is in scope,
a parenthesized expression,
a function call,
an expression accessing the ADM contents of a dataset,
a newly constructed list of ADM instances,
or a newly constructed ADM record.

#### Literals

    Literal ::= StringLiteral
              | <INTEGER_LITERAL>
              | <FLOAT_LITERAL>
              | <DOUBLE_LITERAL>
              | "null"
              | "true"
              | "false"
    StringLiteral ::= <STRING_LITERAL>

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

Querying Big Data is the main point of AsterixDB and AQL.
Data in AsterixDB reside in datasets (collections of ADM records),
each of which in turn resides in some namespace known as a dataverse (data universe).
Data access in a query expression is accomplished via a DatasetAccessExpression.
Dataset access expressions are most commonly used in FLWOR expressions, where variables
are bound to their contents.


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
    RecordConstructor        ::= "{" ( FieldBinding ( "," FieldBinding )* )? "}"
    FieldBinding             ::= Expression ":" Expression

A major feature of AQL is its ability to construct new ADM data instances.
This is accomplished using its constructors for each of the major ADM complex object structures,
namely lists (ordered or unordered) and records.
Ordered lists are like JSON arrays, while unordered lists have bag (multiset) semantics.
Records are built from attributes that are field-name/field-value pairs, again like JSON.
(See the AsterixDB Data Model document for more details on each.)

The following examples illustrate how to construct a new ordered list with 3 items,
a new unordered list with 4 items, and a new record with 2 fields, respectively.
List elements can be homogeneous (as in the first example), which is the common case,
or they may be heterogeneous (as in the second example).
The data values and field name values used to construct lists and records in constructors are all simply AQL expressions.
Thus the list elements, field names, and field values used in constructors can be simple literals (as in these three examples)
or they can come from query variable references or even arbitrarily complex AQL expressions.

##### Examples

    [ "a", "b", "c" ]

    {{ 42, "forty-two", "AsterixDB!", 3.14f }}

    {
      "project name": "AsterixDB"
      "project members": {{ "vinayakb", "dtabass", "chenli" }}
    }

### Path Expressions

    ValueExpr ::= PrimaryExpr ( Field | Index )*
    Field     ::= "." Identifier
    Index     ::= "[" ( Expression | "?" ) "]"

Components of complex types in ADM are accessed via path expressions.
Path access can be applied to the result of an AQL expression that yields an instance of such a type, e.g., a record or list instance.
For records, path access is based on field names.
For ordered lists, path access is based on (zero-based) array-style indexing.
AQL also supports an "I'm feeling lucky" style index accessor, [?], for selecting an arbitrary element from an ordered list.
Attempts to access non-existent fields or list elements produce a null (i.e., missing information) result as opposed to signaling a runtime error.

The following examples illustrate field access for a record, index-based element access for an ordered list, and also a composition thereof.

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
(See the separate document on AsterixDB Similarity Queries for more details on similarity matching.)

An example comparison expression (which yields the boolean value true) is shown below.

##### Example

    5 > 3

### Arithmetic Expressions

    AddExpr  ::= MultExpr ( ( "+" | "-" ) MultExpr )*
    MultExpr ::= UnaryExpr ( ( "*" | "/" | "%" | <CARET> | "idiv" ) UnaryExpr )*
    UnaryExpr ::= ( ( "+" | "-" ) )? ValueExpr

AQL also supports the usual cast of characters for arithmetic expressions.
The example below evaluates to 25.

##### Example

    3 ^ 2 + 4 ^ 2

###  FLWOR Expression

    FLWOR         ::= ( ForClause | LetClause ) ( Clause )* "return" Expression
    Clause         ::= ForClause | LetClause | WhereClause | OrderbyClause
                     | GroupClause | LimitClause | DistinctClause
    ForClause      ::= "for" Variable ( "at" Variable )? "in" ( Expression )
    LetClause      ::= "let" Variable ":=" Expression
    WhereClause    ::= "where" Expression
    OrderbyClause  ::= "order" "by" Expression ( ( "asc" ) | ( "desc" ) )? 
                       ( "," Expression ( ( "asc" ) | ( "desc" ) )? )*
    GroupClause    ::= "group" "by" ( Variable ":=" )? Expression ( "," ( Variable ":=" )? Expression )*
                       "with" VariableRef ( "," VariableRef )*
    LimitClause    ::= "limit" Expression ( "offset" Expression )?
    DistinctClause ::= "distinct" "by" Expression ( "," Expression )*
    Variable       ::= <VARIABLE>

The heart of AQL is the FLWOR (for-let-where-orderby-return) expression.
The roots of this expression were borrowed from the expression of the same name in XQuery.
A FLWOR expression starts with one or more clauses that establish variable bindings.
A for clause binds a variable incrementally to each element of its associated expression;
it includes an optional positional variable for counting/numbering the bindings.
By default no ordering is implied or assumed by a for clause.
A let clause binds a variable to the collection of elements computed by its associated expression.

Following the initial for or let clause(s), a FLWOR expression may contain an arbitrary sequence of other clauses.
The where clause in a FLWOR expression filters the preceding bindings via a boolean expression, much like a where clause does in a SQL query.
The order by clause in a FLWOR expression induces an ordering on the data.
The group by clause, discussed further below, forms groups based on its group by expressions,
optionally naming the expressions' values (which together form the grouping key for the expression).
The with subclause of a group by clause specifies the variable(s) whose values should be grouped based
on the grouping key(s); following the grouping clause, only the grouping key(s) and the variables named
in the with subclause remain in scope, and the named grouping variables now contain lists formed from their input values.
The limit clause caps the number of values returned, optionally starting its result count from a specified offset.
(Web applications can use this feature for doing pagination.)
The distinct clause is similar to the group-by clause, but it forms no groups; it serves only to eliminate duplicate values.
As indicated by the grammar, the clauses in an AQL query can appear in any order.
To interpret a query, one can think of data as flowing down through the query from the first clause to the return clause.

The following example shows a FLWOR expression that selects and returns one user from the dataset FacebookUsers.

##### Example

    for $user in dataset FacebookUsers
    where $user.id = 8
    return $user

The next example shows a FLWOR expression that joins two datasets, FacebookUsers and FacebookMessages,
returning user/message pairs.
The results contain one record per pair, with result records containing the user's name and an entire message.

##### Example

    for $user in dataset FacebookUsers
    for $message in dataset FacebookMessages
    where $message.author-id = $user.id
    return
      {
        "uname": $user.name,
        "message": $message.message
      };

In the next example, a let clause is used to bind a variable to all of a user's FacebookMessages.
The query returns one record per user, with result records containing the user's name and the set of all messages by that user.

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
Null is treated as being smaller than any other value if nulls are encountered in the ordering key(s).

##### Example

      for $user in dataset TwitterUsers
      order by $user.followers_count desc, $user.lang asc
      return $user

The next example illustrates the use of the group by clause in AQL.
After the group by clause in the query, only variables that are either in the group by list or in the with list are in scope.
The variables in the clause's with list will each contain a collection of items following the group by clause;
the collected items are the values that the source variable was bound to in the tuples that formed the group.
Null is handled as a single value for grouping.

##### Example

      for $x in dataset FacebookMessages
      let $messages := $x.message
      group by $loc := $x.sender-location with $messages
      return
        {
          "location" : $loc,
          "message" : $messages
        }

The use of the limit clause is illustrated in thise next example.

##### Example

      for $user in dataset TwitterUsers
      order by $user.followers_count desc
      limit 2
      return $user

The final example shows how AQL's distinct by clause works.
Each variable in scope before the distinct clause is also in scope after the distinct clause.
This clause works similarly to group by, but for each variable that contains more than
one value after the distinct by clause, one value is picked nondeterministically.
(If the variable is in the disctict by list, then its value will be deterministic.)
Nulls are treated as a single value when they occur in a grouping field.

##### Example

      for $x in dataset FacebookMessages
      distinct by $x.sender-location
      return
        {
          "location" : $x.sender-location,
          "message" : $x.message
        }

### Conditional Expression

    IfThenElse ::= "if" "(" Expression ")" "then" Expression "else" Expression

A conditional expression is useful for choosing between two alternative values based on a
boolean condition.  If its first (if) expression is true, its second (then) expression's
value is returned, and otherwise its third (else) expression is returned.

The following example illustrates the form of a conditional expression.
##### Example

    if (2 < 3) then "yes" else "no"

### Quantified Expressions

    QuantifiedExpression ::= ( ( "some" ) | ( "every" ) ) Variable "in" Expression
                             ( "," Variable "in" Expression )* "satisfies" Expression
      
Quantified expressions are used for expressing existential or universal predicates involving the elements of a collection.

The following pair of examples, each of which returns true, illustrate the use of a quantified
expression to test that every (or some) element in the set [1, 2, 3] of integers is less than three.
It is useful to note that if the set were instead the empty set, the first expression would yield true
("every" value in an empty set satisfies the condition) while the second expression would yield false
(since there isn't "some" value, as there are no values in the set, that satisfies the condition).

##### Examples

    every $x in [ 1, 2, 3] satisfies $x < 3
    some $x in [ 1, 2, 3] satisfies $x < 3

## 3. Statements

    Statement ::= ( SingleStatement ( ";" )? )* <EOF>
    SingleStatement ::= DataverseDeclaration
                      | FunctionDeclaration
                      | CreateStatement
                      | DropStatement
                      | LoadStatement
                      | SetStatement
                      | InsertStatement
                      | DeleteStatement
                      | Query

### Declarations
 
    DataverseDeclaration ::= "use" "dataverse" Identifier
    SetStatement         ::= "set" Identifier StringLiteral
    FunctionDeclaration  ::= "declare" "function" Identifier ParameterList "{" Expression "}"
    ParameterList        ::= "(" ( <VARIABLE> ( "," <VARIABLE> )* )? ")"

##### Example

    use dataverse TinySocial;

##### Example

    set simfunction "jaccard";
    set simthreshold "0.6f"; 

##### Example

    set simfunction "jaccard";    
    set simthreshold "0.6f"; 
    
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

#### Dataverses

    DataverseSpecification ::= "dataverse" Identifier IfNotExists ( "with format" StringLiteral )?
    
##### Example

    create dataverse TinySocial;

#### Types

    TypeSpecification    ::= "type" FunctionOrTypeName IfNotExists "as" TypeExpr
    FunctionOrTypeName   ::= QualifiedName
    IfNotExists          ::= ( "if not exists" )?
    TypeExpr             ::= RecordTypeDef | TypeReference | OrderedListTypeDef | UnorderedListTypeDef
    RecordTypeDef        ::= ( "closed" | "open" )? "{" ( RecordField ( "," RecordField )* )? "}"
    RecordField          ::= Identifier ":" ( TypeExpr ) ( "?" )?
    TypeReference        ::= Identifier
    OrderedListTypeDef   ::= "[" ( TypeExpr ) "]"
    UnorderedListTypeDef ::= "{{" ( TypeExpr ) "}}"

##### Example

    create type FacebookUserType as closed {
      id: int32,
      alias: string,
      name: string,
      user-since: datetime,
      friend-ids: {{ int32 }},
      employment: [EmploymentType]
    }

#### Datasets

    DatasetSpecification ::= "internal"? "dataset" QualifiedName "(" Identifier ")" IfNotExists
                             PrimaryKey ( "on" Identifier )? ( "hints" Properties )? 
                           | "external" "dataset" QualifiedName "(" Identifier ")" IfNotExists 
                             "using" AdapterName Configuration ( "hints" Properties )?
    AdapterName          ::= Identifier
    Configuration        ::= "(" ( KeyValuePair ( "," KeyValuePair )* )? ")"
    KeyValuePair         ::= "(" StringLiteral "=" StringLiteral ")"
    Properties           ::= ( "(" Property ( "," Property )* ")" )?
    Property             ::= Identifier "=" ( StringLiteral | <INTEGER_LITERAL> )
    ApplyFunction        ::= "apply" "function" FunctionSignature
    FunctionSignature    ::= FunctionOrTypeName "@" <INTEGER_LITERAL>
    PrimaryKey           ::= "primary" "key" Identifier ( "," Identifier )*

##### Example
    create internal dataset FacebookUsers(FacebookUserType) primary key id;

##### Example

    create external dataset Lineitem(LineitemType) using localfs (
      ("path"="127.0.0.1://SOURCE_PATH"),
      ("format"="delimited-text"),
      ("delimiter"="|"));
      
#### Indices

    IndexSpecification ::= "index" Identifier IfNotExists "on" QualifiedName 
                           "(" ( Identifier ) ( "," Identifier )* ")" ( "type" IndexType )?
    IndexType          ::= "btree"
                         | "rtree"
                         | "keyword"
                         | "fuzzy keyword"
                         | "ngram" "(" <INTEGER_LITERAL> ")"
                         | "fuzzy ngram" "(" <INTEGER_LITERAL> ")"

##### Example

    create index fbAuthorIdx on FacebookMessages(author-id) type btree;

##### Example

    create index fbSenderLocIndex on FacebookMessages(sender-location) type rtree;

##### Example

    create index fbMessageIdx on FacebookMessages(message) type keyword;

#### Functions

    FunctionSpecification ::= "function" FunctionOrTypeName IfNotExists ParameterList "{" Expression "}"
    
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
    
##### Example

    drop dataset FacebookUsers if exists;

##### Example

    drop index fbSenderLocIndex;

##### Example

    drop type FacebookUserType;
    
##### Example

    drop dataverse TinySocial;

##### Example

    drop function add;

### Import/Export Statements

    LoadStatement  ::= "load" "dataset" QualifiedName "using" AdapterName Configuration ( "pre-sorted" )?
    
##### Example

    load dataset FacebookUsers using localfs
    (("path"="localhost:///Users/zuck/AsterixDB/load/fbu.adm"),("format"="adm"));

### Modification Statements

    InsertStatement ::= "insert" "into" "dataset" QualifiedName Query
    DeleteStatement ::= "delete" Variable "from" "dataset" QualifiedName ( "where" Expression )?
    
##### Example

    insert into dataset UsersCopy (for $user in dataset FacebookUsers return $user)

##### Example
    
    delete $user from dataset FacebookUsers where $user.id = 8;
    
### Queries

    Query ::= Expression
    
##### Example
    
    for $praise in {{ "great", "brilliant", "awesome" }}
    return
       string-concat(["AsterixDB is ", $praise]

