# The Asterix Query Language, Version 1.0
## 1. Introduction

This document provides an overview of the Asterix Query language.

## 2. Expressions

    Expression ::= ( OperatorExpr | IfThenElse | FLWOGR | QuantifiedExpression )

### Primary Expressions

    PrimaryExpr ::= Literal | VariableRef | ParenthesizedExpression | FunctionCallExpr
                  | DatasetAccessExpression | ListConstructor | RecordConstructor

#### Literals

    Literal ::= StringLiteral | <INTEGER_LITERAL> | <FLOAT_LITERAL> | <DOUBLE_LITERAL> | <NULL> | <TRUE> | <FALSE>
    StringLiteral ::= <STRING_LITERAL>

#### Variable References

    VariableRef ::= <VARIABLE>
    
#### Parenthesized Expressions
    
    ParenthesizedExpression ::= <LEFTPAREN> Expression <RIGHTPAREN>

#### Function Calls

    FunctionCallExpr ::= FunctionOrTypeName <LEFTPAREN> ( Expression ( "," Expression )* )? <RIGHTPAREN>
    
#### Dataset Access

    DatasetAccessExpression ::= <DATASET> ( ( Identifier ( "." Identifier )? )
                              | ( <LEFTPAREN> Expression ( "," Expression )* <RIGHTPAREN> ) )
    Identifier              ::= <IDENTIFIER> | StringLiteral

#### Constructors

    ListConstructor          ::= ( OrderedListConstructor | UnorderedListConstructor )
    OrderedListConstructor   ::= "[" ( Expression ( "," Expression )* )? "]"
    UnorderedListConstructor ::= "{{" ( Expression ( "," Expression )* )? "}}"
    RecordConstructor        ::= "{" ( FieldBinding ( "," FieldBinding )* )? "}"
    FieldBinding             ::= Expression ":" Expression

### Path Expressions

    ValueExpr ::= PrimaryExpr ( Field | Index )*
    Field     ::= "." Identifier
    Index     ::= "[" ( Expression | "?" ) "]"

### Logical Expressions

    OperatorExpr ::= AndExpr ( "or" AndExpr )*
    AndExpr      ::= RelExpr ( "and" RelExpr )*
    
### Comparison Expressions

    RelExpr ::= AddExpr ( ( "<" | ">" | "<=" | ">=" | "=" | "!=" | "~=" ) AddExpr )?

### Arithmetic Expressions

    AddExpr  ::= MultExpr ( ( "+" | "-" ) MultExpr )*
    MultExpr ::= UnaryExpr ( ( "*" | "/" | "%" | <CARET> | "idiv" ) UnaryExpr )*
    UnaryExpr ::= ( ( "+" | "-" ) )? ValueExpr

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


### Conditional Expression
    
    IfThenElse ::= "if" <LEFTPAREN> Expression <RIGHTPAREN> "then" Expression "else" Expression


### Quantified Expressions
    
    QuantifiedExpression ::= ( ( "some" ) | ( "every" ) ) Variable "in" Expression 
                             ( "," Variable "in" Expression )* "satisfies" Expression


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
                      | FeedStatement
                      | Query
    
### Declarations    
    
    DataverseDeclaration ::= "use" "dataverse" Identifier
    SetStatement         ::= "set" Identifier StringLiteral
    FunctionDeclaration  ::= "declare" "function" Identifier <LEFTPAREN> ( <VARIABLE> ( "," <VARIABLE> )* )? <RIGHTPAREN> "{" Expression "}"

### Lifecycle Management Statements

    CreateStatement ::= "create" ( TypeSpecification | DatasetSpecification | IndexSpecification | DataverseSpecification | FunctionSpecification )

    DropStatement       ::= "drop" ( <DATASET> QualifiedName IfExists
                                   | "index" DoubleQualifiedName IfExists
                                   | "type" FunctionOrTypeName IfExists
                                   | "dataverse" Identifier IfExists
                                   | "function" FunctionSignature IfExists )
    IfExists            ::= ( "if" "exists" )?
    QualifiedName       ::= Identifier ( "." Identifier )?
    DoubleQualifiedName ::= Identifier "." Identifier ( "." Identifier )?

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
    
#### Datasets

    DatasetSpecification ::= "external" <DATASET> QualifiedName <LEFTPAREN> Identifier <RIGHTPAREN> IfNotExists 
                                 "using" AdapterName Configuration ( "hints" Properties )? 
                           | "feed" <DATASET> QualifiedName <LEFTPAREN> Identifier <RIGHTPAREN> IfNotExists
                                 "using" AdapterName Configuration ( ApplyFunction )? PrimaryKey ( "on" Identifier )? ( "hints" Properties )? 
                           | <DATASET> QualifiedName <LEFTPAREN> Identifier <RIGHTPAREN> IfNotExists
                             PrimaryKey ( "on" Identifier )? ( "hints" Properties )?
    AdapterName          ::= Identifier
    Configuration        ::= <LEFTPAREN> ( KeyValuePair ( "," KeyValuePair )* )? <RIGHTPAREN>
    KeyValuePair         ::= <LEFTPAREN> StringLiteral "=" StringLiteral <RIGHTPAREN>
    Properties           ::= ( <LEFTPAREN> Property ( "," Property )* <RIGHTPAREN> )?
    Property             ::= Identifier "=" ( StringLiteral | <INTEGER_LITERAL> )
    ApplyFunction        ::= "apply" "function" FunctionSignature
    FunctionSignature    ::= FunctionOrTypeName "@" <INTEGER_LITERAL>
    PrimaryKey           ::= "primary" "key" Identifier ( "," Identifier )*

#### Indices

    IndexSpecification ::= "index" Identifier IfNotExists "on" QualifiedName <LEFTPAREN> ( Identifier ) ( "," Identifier )* <RIGHTPAREN> ( "type" IndexType )?
    IndexType          ::= "btree" | "rtree" | "keyword" | "fuzzy keyword" | "ngram" <LEFTPAREN> <INTEGER_LITERAL> <RIGHTPAREN> | "fuzzy ngram" <LEFTPAREN> <INTEGER_LITERAL> <RIGHTPAREN>

#### Dataverses

    DataverseSpecification ::= "dataverse" Identifier IfNotExists ( "with format" StringLiteral )?

#### Functions

    FunctionSpecification ::= "function" FunctionOrTypeName IfNotExists <LEFTPAREN> ( <VARIABLE> ( "," <VARIABLE> )* )? <RIGHTPAREN> "{" Expression "}"
    

### Import/Export Statements

    LoadStatement  ::= "load" <DATASET> QualifiedName "using" AdapterName Configuration ( "pre-sorted" )?

### Modification Statements

    InsertStatement ::= "insert" "into" <DATASET> QualifiedName Query
    DeleteStatement ::= "delete" Variable "from" <DATASET> QualifiedName ( "where" Expression )?

### Feed Management Statements

    FeedStatement ::= "begin" "feed" QualifiedName
                    | "suspend" "feed" QualifiedName
                    | "resume" "feed" QualifiedName
                    | "end" "feed" QualifiedName
                    | "alter" "feed" QualifiedName "set" Configuration

### Queries

    Query ::= Expression
    
