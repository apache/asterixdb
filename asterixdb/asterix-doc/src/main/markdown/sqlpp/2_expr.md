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

# <a id="Expressions">2. Expressions</a>

    Expression ::= OperatorExpression | CaseExpression | QuantifiedExpression

SQL++ is a highly composable expression language. Each SQL++ expression returns zero or more data model instances. There are three major kinds of expressions in SQL++. At the topmost level, a SQL++ expression can be an OperatorExpression (similar to a mathematical expression), an ConditionalExpression (to choose between alternative values), or a QuantifiedExpression (which yields a boolean value). Each will be detailed as we explore the full SQL++ grammar.

## <a id="Primary_expressions">Primary Expressions</a>

    PrimaryExpr ::= Literal
                  | VariableReference
                  | ParenthesizedExpression
                  | FunctionCallExpression
                  | Constructor

The most basic building block for any SQL++ expression is PrimaryExpression. This can be a simple literal (constant)
value, a reference to a query variable that is in scope, a parenthesized expression, a function call, or a newly
constructed instance of the data model (such as a newly constructed object, array, or multiset of data model instances).

### <a id="Literals">Literals</a>

    Literal        ::= StringLiteral
                       | IntegerLiteral
                       | FloatLiteral
                       | DoubleLiteral
                       | <NULL>
                       | <MISSING>
                       | <TRUE>
                       | <FALSE>
    StringLiteral  ::= "\'" (<ESCAPE_APOS> | ~["\'"])* "\'"
                       | "\"" (<ESCAPE_QUOT> | ~["\'"])* "\""
    <ESCAPE_APOS>  ::= "\\\'"
    <ESCAPE_QUOT>  ::= "\\\""
    IntegerLiteral ::= <DIGITS>
    <DIGITS>       ::= ["0" - "9"]+
    FloatLiteral   ::= <DIGITS> ( "f" | "F" )
                     | <DIGITS> ( "." <DIGITS> ( "f" | "F" ) )?
                     | "." <DIGITS> ( "f" | "F" )
    DoubleLiteral  ::= <DIGITS>
                     | <DIGITS> ( "." <DIGITS> )?
                     | "." <DIGITS>

Literals (constants) in SQL++ can be strings, integers, floating point values, double values, boolean constants, or special constant values like `NULL` and `MISSING`. The `NULL` value is like a `NULL` in SQL; it is used to represent an unknown field value. The specialy value `MISSING` is only meaningful in the context of SQL++ field accesses; it occurs when the accessed field simply does not exist at all in a object being accessed.

The following are some simple examples of SQL++ literals.

##### Examples

    'a string'
    "test string"
    42

Different from standard SQL, double quotes play the same role as single quotes and may be used for string literals in SQL++.

### <a id="Variable_references">Variable References</a>

    VariableReference     ::= <IDENTIFIER>|<DelimitedIdentifier>
    <IDENTIFIER>          ::= <LETTER> (<LETTER> | <DIGIT> | "_" | "$")*
    <LETTER>              ::= ["A" - "Z", "a" - "z"]
    DelimitedIdentifier   ::= "\`" (<ESCAPE_APOS> | ~["\'"])* "\`"

A variable in SQL++ can be bound to any legal data model value. A variable reference refers to the value to which an in-scope variable is bound. (E.g., a variable binding may originate from one of the `FROM`, `WITH` or `LET` clauses of a `SELECT` statement or from an input parameter in the context of a function body.) Backticks, e.g., \`id\`, are used for delimited identifiers. Delimiting is needed when a variable's desired name clashes with a SQL++ keyword or includes characters not allowed in regular identifiers.

##### Examples

    tweet
    id
    `SELECT`
    `my-function`

### <a id="Parenthesized_expressions">Parenthesized expressions</a>

    ParenthesizedExpression ::= "(" Expression ")" | Subquery

An expression can be parenthesized to control the precedence order or otherwise clarify a query. In SQL++, for composability, a subquery is also an parenthesized expression.

The following expression evaluates to the value 2.

##### Example

    ( 1 + 1 )

### <a id="Function_call_expressions">Function call expressions</a>

    FunctionCallExpression ::= FunctionName "(" ( Expression ( "," Expression )* )? ")"

Functions are included in SQL++, like most languages, as a way to package useful functionality or to componentize complicated or reusable SQL++ computations. A function call is a legal SQL++ query expression that represents the value resulting from the evaluation of its body expression with the given parameter bindings; the parameter value bindings can themselves be any SQL++ expressions.

The following example is a (built-in) function call expression whose value is 8.

##### Example

    length('a string')

### <a id="Constructors">Constructors</a>

    CollectionConstructor    ::= ArrayConstructor | MultisetConstructor
    ArrayConstructor         ::= "[" ( Expression ( "," Expression )* )? "]"
    MultisetConstructor      ::= "{{" ( Expression ( "," Expression )* )? "}}"
    ObjectConstructor        ::= "{" ( FieldBinding ( "," FieldBinding )* )? "}"
    FieldBinding             ::= Expression ":" Expression

A major feature of SQL++ is its ability to construct new data model instances. This is accomplished using its constructors
for each of the model's complex object structures, namely arrays, multisets, and objects.
Arrays are like JSON arrays, while multisets have bag semantics.
Objects are built from fields that are field-name/field-value pairs, again like JSON.
(See the [data model document](../datamodel.html) for more details on each.)

The following examples illustrate how to construct a new array with 3 items, a new object with 2 fields,
and a new multiset with 4 items, respectively. Array elements or multiset elements can be homogeneous (as in
the first example),
which is the common case, or they may be heterogeneous (as in the third example). The data values and field name values
used to construct arrays, multisets, and objects in constructors are all simply SQL++ expressions. Thus, the collection elements,
field names, and field values used in constructors can be simple literals or they can come from query variable references
or even arbitrarily complex SQL++ expressions (subqueries).

##### Examples

    [ 'a', 'b', 'c' ]

    {
      'project name': 'Hyracks',
      'project members': [ 'vinayakb', 'dtabass', 'chenli', 'tsotras', 'tillw' ]
    }

    {{ 42, "forty-two!", { "rank": "Captain", "name": "America" }, 3.14159 }}

### <a id="Path_expressions">Path expressions</a>

    PathExpression  ::= PrimaryExpression ( Field | Index )*
    Field           ::= "." Identifier
    Index           ::= "[" ( Expression | "?" ) "]"

Components of complex types in the data model are accessed via path expressions. Path access can be applied to the result
of a SQL++ expression that yields an instance of  a complex type, e.g., a object or array instance. For objects,
path access is based on field names. For arrays, path access is based on (zero-based) array-style indexing.
SQL++ also supports an "I'm feeling lucky" style index accessor, [?], for selecting an arbitrary element from an array.
 Attempts to access non-existent fields or out-of-bound array elements produce the special value `MISSING`.

The following examples illustrate field access for a object, index-based element access for an array, and also a
composition thereof.

##### Examples

    ({"name": "MyABCs", "array": [ "a", "b", "c"]}).array

    (["a", "b", "c"])[2]

    ({"name": "MyABCs", "array": [ "a", "b", "c"]}).array[2]

### <a id="Operator_expressions">Operator expressions</a>

Operators perform a specific operation on the input values or expressions. The syntax of an operator expression is as follows:

    OperatorExpression ::= PathExpression
                           | Operator OperatorExpression
                           | OperatorExpression Operator (OperatorExpression)?
                           | OperatorExpression <BETWEEN> OperatorExpression <AND> OperatorExpression

SQL++ provides a full set of operators that you can use within its statements. Here are the categories of operators:

* [Arithmetic operators](#Arithmetic_operators), to perform basic mathematical operations;
* [Collection operators](#Collection_operators), to evaluate expressions on collections or objects;
* [Comparison operators](#Comparison_operators), to compare two expressions;
* [Logical Operators](#Logical_operators), to combine operators using Boolean logic.

The following table summarizes the precedence order (from higher to lower) of the major unary and binary operators:

| Operator                                                                    | Operation |
|-----------------------------------------------------------------------------|-----------|
| EXISTS, NOT EXISTS                                                          |  collection emptiness testing |
| ^                                                                           |  exponentiation  |
| *, /                                                                        |  multiplication, division |
| +, -                                                                        |  addition, subtraction  |
| &#124;&#124;                                                                          |  string concatenation |
| IS NULL, IS NOT NULL, IS MISSING, IS NOT MISSING, <br/>IS UNKNOWN, IS NOT UNKNOWN| unknown value comparison |
| BETWEEN, NOT BETWEEN                                                        | range comparison (inclusive on both sides) |
| =, !=, <, >, <=, >=, LIKE, NOT LIKE, IN, NOT IN                             | comparison  |
| NOT                                                                         | logical negation |
| AND                                                                         | conjunction |
| OR                                                                          | disjunction |

### <a id="Arithmetic_operators">Arithmetic operators</a>
Arithemtic operators are used to exponentiate, add, subtract, multiply, and divide numeric values, or concatenate string values.

| Operator     |  Purpose                                                                | Example    |
|--------------|-------------------------------------------------------------------------|------------|
| +, -         |  As unary operators, they denote a <br/>positive or negative expression | SELECT VALUE -1; |
| +, -         |  As binary operators, they add or subtract                              | SELECT VALUE 1 + 2; |
| *, /         |  Multiply, divide                                                       | SELECT VALUE 4 / 2.0; |
| ^            |  Exponentiation                                                         | SELECT VALUE 2^3;       |
| &#124;&#124; |  String concatenation                                                   | SELECT VALUE "ab"&#124;&#124;"c"&#124;&#124;"d";       |

### <a id="Collection_operators">Collection operators</a>
Collection operators are used for membership tests (IN, NOT IN) or empty collection tests (EXISTS, NOT EXISTS).

| Operator   |  Purpose                                     | Example    |
|------------|----------------------------------------------|------------|
| IN         |  Membership test                             | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.lang IN ["en", "de"]; |
| NOT IN     |  Non-membership test                         | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.lang NOT IN ["en"]; |
| EXISTS     |  Check whether a collection is not empty     | SELECT * FROM ChirpMessages cm <br/>WHERE EXISTS cm.referredTopics; |
| NOT EXISTS |  Check whether a collection is empty         | SELECT * FROM ChirpMessages cm <br/>WHERE NOT EXISTS cm.referredTopics; |

### <a id="Comparison_operators">Comparison operators</a>
Comparison operators are used to compare values. The comparison operators fall into one of two sub-categories: missing value comparisons and regular value comparisons. SQL++ (and JSON) has two ways of representing missing information in a object - the presence of the field with a NULL for its value (as in SQL), and the absence of the field (which JSON permits). For example, the first of the following objects represents Jack, whose friend is Jill. In the other examples, Jake is friendless a la SQL, with a friend field that is NULL, while Joe is friendless in a more natural (for JSON) way, i.e., by not having a friend field.

##### Examples
{"name": "Jack", "friend": "Jill"}

{"name": "Jake", "friend": NULL}

{"name": "Joe"}

The following table enumerates all of SQL++'s comparison operators.

| Operator       |  Purpose                                   | Example    |
|----------------|--------------------------------------------|------------|
| IS NULL        |  Test if a value is NULL                       | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.name IS NULL; |
| IS NOT NULL    |  Test if a value is not NULL                   | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.name IS NOT NULL; |
| IS MISSING     |  Test if a value is MISSING                    | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.name IS MISSING; |
| IS NOT MISSING |  Test if a value is not MISSING                | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.name IS NOT MISSING;|
| IS UNKNOWN     |  Test if a value is NULL or MISSING            | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.name IS UNKNOWN; |
| IS NOT UNKNOWN |  Test if a value is neither NULL nor MISSING   | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.name IS NOT UNKNOWN;|
| BETWEEN        |  Test if a value is between a start value and <br/>a end value. The comparison is inclusive <br/>to both start and end values. |  SELECT * FROM ChirpMessages cm <br/>WHERE cm.chirpId BETWEEN 10 AND 20;|
| =              |  Equality test                                 | SELECT * FROM ChirpMessages cm <br/>WHERE cm.chirpId=10; |
| !=             |  Inequality test                               | SELECT * FROM ChirpMessages cm <br/>WHERE cm.chirpId!=10;|
| <              |  Less than                                     | SELECT * FROM ChirpMessages cm <br/>WHERE cm.chirpId<10; |
| >              |  Greater than                                  | SELECT * FROM ChirpMessages cm <br/>WHERE cm.chirpId>10; |
| <=             |  Less than or equal to                         | SELECT * FROM ChirpMessages cm <br/>WHERE cm.chirpId<=10; |
| >=             |  Greater than or equal to                      | SELECT * FROM ChirpMessages cm <br/>WHERE cm.chirpId>=10; |
| LIKE           |  Test if the left side matches a<br/> pattern defined on the right<br/> side; in the pattern,  "%" matches  <br/>any string while "&#95;" matches <br/> any character. | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.name LIKE "%Giesen%";|
| NOT LIKE       |  Test if the left side does not <br/>match a pattern defined on the right<br/> side; in the pattern,  "%" matches <br/>any string while "&#95;" matches <br/> any character. | SELECT * FROM ChirpMessages cm <br/>WHERE cm.user.name NOT LIKE "%Giesen%";|

The following table summarizes how the missing value comparison operators work.

| Operator | Non-NULL/Non-MISSING value | NULL | MISSING |
|----------|----------------|------|---------|
| IS NULL  | FALSE | TRUE | MISSING |
| IS NOT NULL | TRUE | FALSE | MISSING |
| IS MISSING  | FALSE | FALSE | TRUE |
| IS NOT MISSING | TRUE | TRUE | FALSE |
| IS UNKNOWN | FALSE | TRUE | TRUE |
| IS NOT UNKNOWN | TRUE | FALSE | FALSE|

### <a id="Logical_operators">Logical operators</a>
Logical operators perform logical `NOT`, `AND`, and `OR` operations over Boolean values (`TRUE` and `FALSE`) plus `NULL` and `MISSING`.

| Operator |  Purpose                                   | Example    |
|----------|-----------------------------------------------------------------------------|------------|
| NOT      |  Returns true if the following condition is false, otherwise returns false  | SELECT VALUE NOT TRUE;  |
| AND      |  Returns true if both branches are true, otherwise returns false            | SELECT VALUE TRUE AND FALSE; |
| OR       |  Returns true if one branch is true, otherwise returns false                | SELECT VALUE FALSE OR FALSE; |

The following table is the truth table for `AND` and `OR`.

| A  | B  | A AND B  | A OR B |
|----|----|----------|--------|
| TRUE | TRUE | TRUE | TRUE |
| TRUE | FALSE | FALSE | TRUE |
| TRUE | NULL | NULL | TRUE |
| TRUE | MISSING | MISSING | TRUE |
| FALSE | FALSE | FALSE | FALSE |
| FALSE | NULL | FALSE | NULL |
| FALSE | MISSING | FALSE | MISSING |
| NULL | NULL | NULL | NULL |
| NULL | MISSING | MISSING | NULL |
| MISSING | MISSING | MISSING | MISSING |

The following table demonstrates the results of `NOT` on all possible inputs.

| A  | NOT A |
|----|----|
| TRUE | FALSE |
| FALSE | TRUE |
| NULL | NULL |
| MISSING | MISSING |

### <a id="Case_expressions">Case expressions</a>

    CaseExpression ::= SimpleCaseExpression | SearchedCaseExpression
    SimpleCaseExpression ::= <CASE> Expression ( <WHEN> Expression <THEN> Expression )+ ( <ELSE> Expression )? <END>
    SearchedCaseExpression ::= <CASE> ( <WHEN> Expression <THEN> Expression )+ ( <ELSE> Expression )? <END>

In a simple `CASE` expression, the query evaluator searches for the first `WHEN` ... `THEN` pair in which the `WHEN` expression is equal to the expression following `CASE` and returns the expression following `THEN`. If none of the `WHEN` ... `THEN` pairs meet this condition, and an `ELSE` branch exists, it returns the `ELSE` expression. Otherwise, `NULL` is returned.

In a searched CASE expression, the query evaluator searches from left to right until it finds a `WHEN` expression that is evaluated to `TRUE`, and then returns its corresponding `THEN` expression. If no condition is found to be `TRUE`, and an `ELSE` branch exists, it returns the `ELSE` expression. Otherwise, it returns `NULL`.

The following example illustrates the form of a case expression.
##### Example

    CASE (2 < 3) WHEN true THEN "yes" ELSE "no" END

### <a id="Quantified_expressions">Quantified expressions</a>

    QuantifiedExpression ::= ( (<ANY>|<SOME>) | <EVERY> ) Variable <IN> Expression ( "," Variable "in" Expression )*
                             <SATISFIES> Expression (<END>)?

Quantified expressions are used for expressing existential or universal predicates involving the elements of a collection.

The following pair of examples illustrate the use of a quantified expression to test that every (or some) element in the set [1, 2, 3] of integers is less than three. The first example yields `FALSE` and second example yields `TRUE`.

It is useful to note that if the set were instead the empty set, the first expression would yield `TRUE` ("every" value in an empty set satisfies the condition) while the second expression would yield `FALSE` (since there isn't "some" value, as there are no values in the set, that satisfies the condition).

##### Examples

    EVERY x IN [ 1, 2, 3 ] SATISFIES x < 3
    SOME x IN [ 1, 2, 3 ] SATISFIES x < 3


