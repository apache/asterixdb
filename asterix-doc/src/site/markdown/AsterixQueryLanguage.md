`<wiki:toc max_depth="2" />`

# The Asterix Query Language, Version 1.0 #

# Introduction #

This wiki page provides an overview of the Asterix Query language and the Asterix Data model.

*WARNING:* _THIS IS AN INCOMPLETE SUSPENDED WORK IN PROGRESS...
_  It will hopefully be resumed shortly in order to produce a legit AQL spec to go out with the Beta Release of AsterixDB.  What's here is very likely inconsistent with what's in the system as of today, as this was from an older snapshot of the world.

# Asterix Data Model #

Data in Asterix is represented using the Asterix Data Model (ADM). The ADM derives inspiration from prior standards such as JSON, XQuery, and the Object Data Model from ODMG.

## Asterix Types ##

### Primitive Types ##

|| *Primitive Type* || *Description* ||
|| int8   || Signed 8-bit integer. Valid range -128 thru 127 ||
|| int16  || Signed 16-bit integer. Valid range -32768 thru 32767 ||
|| int32  || Signed 32-bit integer. Valid range -2147483648 thru 2147483647 ||
|| int64  || Signed 64-bit integer. Valid range -9223372036854775807 thru 9223372036854775808 ||
|| uint8  || Unsigned 8-bit integer. Valid range 0 thru 255 ||
|| uint16 || Unsigned 16-bit integer. Valid range 0 thru 65535 ||
|| uint32 || Unsigned 32-bit integer. Valid range 0 thru 4294967295 ||
|| uint64 || Unsigned 64-bit integer. Valid range 0 thru 18446744073709551615 ||
|| string || String of characters ||
|| null   || null type (Type of the null value) ||
|| date   || Date ||
|| time   || Time of day ||
|| boolean || Boolean ||
|| datetime || Date and time ||
|| point2d || A point in 2-D space ||
|| point3d || A point in 3-D space ||
|| binary || Binary data ||
|| yminterval || Year-Month interval ||
|| dtinterval || Day-Time interval ||
|| interval || Year-Month and Day-Time interval ||

### Collection Types ###

|| *Collection Type* || *Description* ||
|| Record || A record type describes the record data item. A record contains a set of fields which can have values of any ADM type. Fields of a record must be unique. ||
|| Union || A union type is an abstract type (A value never has a union type) that describes a set of type choices. ||
|| Ordered List || An orderedlist instance represents a sequence of values where the order of the instances is determined by creation/insertion ||
|| UnorderedList || An unorderedlist instance represents a collection of values where the order of the instances where the order is irrelevant ||
|| Enumeration || An enumeration type represents a choice of string values ||

# AQL Expressions #

## Primary Expressions ##

Primary expressions are the basic expressions that form the core of AQL.

### Literals ###

A Literal is a syntactic representation of a constant value. The various literals allowed in AQL are described in the table below.

|| *Literal type* || *Syntax* ||
|| StringLiteral || ` STRING_LITERAL : ("\"" ("\\\"" | ~["\""])* "\"") | ("\'"("\\\'" | ~["\'"])* "\'") ` ||
|| IntegerLiteral || ` INTEGER_LITERAL : (["0" - "9"])+ ` ||
|| FloatLiteral || ` FLOAT_LITERAL: ((["0" - "9"])* "." (["0" - "9"])+ ("f" | "F")) ` ||
|| DoubleLiteral || ` DOUBLE_LITERAL: ((["0" - "9"])* "." (["0" - "9"])+) ` ||
|| NullLiteral || ` NULL_LITERAL: "null" ` ||
|| BooleanLiteral || ` BOOLEAN_LITERAL: "true" | "false" ` ||

### Function Call ###

Function Calls in AQL can be used to invoke builtin functions as well as user defined functions.
Function Calls have the following syntax.


            IDENTIFIER "(" ( Expression ( "," Expression )* )? ")"


### Variable Reference ###

Variables in AQL are used to bind to values. Variables can be bound to values by the For, Let, Group by clauses of the FLWOR expressions. Variables can also be bound by
the Quantified Expressions.

### Ordered List Constructor ###

Constructs an ordered list. An ordered list represents a collection of values. The order of values is relevant. The collection may contain duplicate values.

### Unordered List Constructor ###

Constructs an unordered list. An unordered list represents a collection of values. The order of values is not relevant. The collection may contain duplicate values.

### Record Constructor ###

Constructs an AQL Record. A record contains fields. Each field has a name and a value. The name of the field is of type string. The value of a field may be any legal ADM data type. A record may not contain duplicate fields.

## Arithmetic Expressions ##

AQL allows all the standard arithmetic operators on numeric data types. The specific operators allowed are:

|| *Operator* || *Description* ||
|| + || Add ||
|| - || Subtract ||
|| * || Multiply ||
|| / || Divide ||
|| mod || Modulo ||

## Comparison Expressions ##

AQL provides the six standard comparison expressions listed below. In addition, AQL supports fuzzy comparisons.

|| *Operator* || *Description* ||
|| = || Equal ||
|| = || Not Equal ||
|| `< || Less Than ||
|| `<= || Less Than or Equal ||
|| >` || Greater Than ||
|| >`= || Greater Than or Equal ||
|| >`= || Greater Than or Equal ||
|| ~= || Fuzzy Equals ||

## Logical Expressions ##

AQL provides two logical connectors:

|| *Operator* || *Description* ||
|| and || Logical AND ||
|| or || Logical OR ||

## Field Access Expressions ##

The "." operator is used to access fields of a record. For example,


            $x.name


accesses the name field of the record bound to $x.

## Indexed Expressions ##

Indexed expressions are used to access values in an ordered list. For example,


            $x[5]


accesses the 6th item in the list bound to $x. Indexes start at 0.

## FLWOR Expression ##

The FLWOR expression is the most elaborate expression in AQL. It is made up of two parts -- Clauses and the Return Expression.

The syntax of the FLWOR expression is:


        
        ( ForClause | LetClause )
        ( ForClause | LetClause | WhereClause | OrderClause | GroupClause | LimitClause | DistinctClause )*
        "return" ReturnExpression
        


* For Clause

        "for" Variable "in" Expression

* Let Clause

        "let" Variable ":=" Expression

* Where Clause

        "where" Expression

* Order Clause

        "order" "by" Expression ("asc" | "desc") ("," Expression ("asc" | "desc"))*

* Group Clause

        "group" "by" ((Variable ":=")? Expression) ("," ((Variable ":=")? Expression))* "with" Variable

* Limit Clause

        "limit" Expression ("," Expression)?


* If Then Else Expressions *

        "if" "(" Expression ")" "then" Expression "else" Expression


* Quantified Expressions *

        ("some" | "every") Variable "in" Expression "satisfies" Expression

