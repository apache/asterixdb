# The Asterix Query Language, Version 1.0
## 1. Introduction

This document provides an overview of the Asterix Query language.


## 2. Expressions

    Expression ::= ( OperatorExpr | IfThenElse | FLWOGR | QuantifiedExpression )


### Primary Expressions

    PrimaryExpr ::= Literal
                  | VariableRef
                  | ParenthesizedExpression
                  | FunctionCallExpr
                  | DatasetAccessExpression
                  | ListConstructor
                  | RecordConstructor
                  

#### Literals

    Literal ::= StringLiteral
              | <INTEGER_LITERAL>
              | <FLOAT_LITERAL>
              | <DOUBLE_LITERAL>
              | "null"
              | "true"
              | "false"
    StringLiteral ::= <STRING_LITERAL>

##### Examples

    "a string"
    42


#### Variable References

    VariableRef ::= <VARIABLE>

##### Example

    $id  
    

#### Parenthesized Expressions
    
    ParenthesizedExpression ::= "(" Expression ")"

##### Example

    ( 1 + 1 )


#### Function Calls

    FunctionCallExpr ::= FunctionOrTypeName "(" ( Expression ( "," Expression )* )? ")"

##### Example

    string-length("a string")


#### Dataset Access

    DatasetAccessExpression ::= "dataset" ( ( Identifier ( "." Identifier )? )
                              | ( "(" Expression ")" ) )
    Identifier              ::= <IDENTIFIER> | StringLiteral

##### Examples

    dataset customers
    dataset (string-join("customers", $country))
    

#### Constructors

    ListConstructor          ::= ( OrderedListConstructor | UnorderedListConstructor )
    OrderedListConstructor   ::= "[" ( Expression ( "," Expression )* )? "]"
    UnorderedListConstructor ::= "{{" ( Expression ( "," Expression )* )? "}}"
    RecordConstructor        ::= "{" ( FieldBinding ( "," FieldBinding )* )? "}"
    FieldBinding             ::= Expression ":" Expression

##### Examples

    [ "a", "b", "c" ]
    
    {{ 42, "forty-two", "AsterixDB!" }}
    
    {
      "project name"    : "AsterixDB"
      "project members" : {{ "vinayakb", "dtabass", "chenli" }}
    } 


### Path Expressions

    ValueExpr ::= PrimaryExpr ( Field | Index )*
    Field     ::= "." Identifier
    Index     ::= "[" ( Expression | "?" ) "]"

##### Examples

    { "list" : [ "a", "b", "c"] }.list
    
    [ "a", "b", "c"][2]
    
    { "list" : [ "a", "b", "c"] }.list[2]


### Logical Expressions

    OperatorExpr ::= AndExpr ( "or" AndExpr )*
    AndExpr      ::= RelExpr ( "and" RelExpr )*
    
##### Example

    $a > 3 and $a < 5
    

### Comparison Expressions

    RelExpr ::= AddExpr ( ( "<" | ">" | "<=" | ">=" | "=" | "!=" | "~=" ) AddExpr )?
    
##### Example

    5 > 3


### Arithmetic Expressions

    AddExpr  ::= MultExpr ( ( "+" | "-" ) MultExpr )*
    MultExpr ::= UnaryExpr ( ( "*" | "/" | "%" | <CARET> | "idiv" ) UnaryExpr )*
    UnaryExpr ::= ( ( "+" | "-" ) )? ValueExpr

##### Example

    3 ^ 2 + 4 ^ 2


###  FLWOGR Expression   
    
    FLWOGR         ::= ( ForClause | LetClause ) ( Clause )* "return" Expression
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


##### Example

    for $user in dataset FacebookUsers
    where $user.id = 8
    return $user
    
##### Example

    for $user in dataset FacebookUsers
    for $message in dataset FacebookMessages
    where $message.author-id = $user.id
    return
      {
        "uname": $user.name,
        "message": $message.message
      }; 
    
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
      
##### Example
      
      for $user in dataset TwitterUsers
      order by $user.followers_count desc, $user.lang asc
      return $user
      
* null is smaller than any other value

##### Example

      for $x in dataset FacebookMessages
      let $messages := $x.message
      group by $loc := $x.sender-location with $messages
      return
        {
          "location" : $loc,
          "message" : $messages
        }

* after group by only variables that are either in the group-by-list or in the with-list are in scope
* the variables in the with-clause contain a collection of items after the group by clause  (all the values that the variable was bound to in the tuples that make up the group)
* null is handled as a single value for grouping

##### Example

      for $user in dataset TwitterUsers
      order by $user.followers_count desc
      limit 2
      return $user

##### Example (currently not working)
    
      for $x in dataset FacebookMessages
      distinct by $x.sender-location
      return
        {
          "location" : $x.sender-location,
          "message" : $x.message
        }

* every variable that is in-scope before the distinct clause is also in scope after the distinct clause
* works a lot like group by, but for every variable that contains more than one value after the distinct-by clause, one value is picked non-deterministically
* if the variable is in the disctict-by list, then value is deterministic
* null is a single value
    
### Conditional Expression
    
    IfThenElse ::= "if" "(" Expression ")" "then" Expression "else" Expression

##### Example

    if (2 < 3) then "yes" else "no"


### Quantified Expressions
    
    QuantifiedExpression ::= ( ( "some" ) | ( "every" ) ) Variable "in" Expression 
                             ( "," Variable "in" Expression )* "satisfies" Expression
                             
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
       string-concat(["AsterixDB is ", $praise])
