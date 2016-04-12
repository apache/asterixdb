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

Lexer Generator
===============

This tool automate the creation of Hand-Coded-Like Lexers.   
It was created to address the performance issues of other (more advanced) lexer generators like JavaCC that arise when you need to scan TB of data. In particular it is *~20x faster* than javacc and typically can parse the data from a normal harddisk at *more than 70MBs*.


Maven Plugin (to put inside pom.xml)
-------------------------------------
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>2.0.2</version>
                <configuration>
                    <source>1.6</source>
                    <target>1.6</target>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.asterix</groupId>
                <artifactId>lexer-generator-maven-plugin</artifactId>
                <version>0.1-SNAPSHOT</version>
                <configuration>
                    <grammarFile>src/main/java/org.apache/asterix/runtime/operators/file/adm/adm.grammar</grammarFile>
                    <outputDir>${project.build.directory}/generated-sources</outputDir>
                </configuration>
                <executions>
                    <execution>
                        <id>generate-lexer</id>
                        <phase>generate-sources</phase>
                        <goals>
                            <goal>generate-lexer</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>


Command line
-------------
    LexerGenerator\nusage: java LexerGenerator <configuration file>



What means Hand-Coded-Like and why it is so fast
------------------------------------------------
The most of the Lexers use a Finite State Machine encoded in data structure called [State Transition Table](http://en.wikipedia.org/wiki/State_transition_table).   
While elegant and practical this approach require some extra controls and operations to deal with the data structure at runtime. A different approach consists in encoding the State Machine as actual code, in this way all the operations done are limited to the minumum amount needed to parse our grammar.   
A common problem with this kind of hard-hand-coded lexers is that is almost impossible to do manutency and changes, this is the reason of this Lexer Generator able to produce a Hand-Coded-Like lexer starting from a grammar specification.

Another big difference with the most of the LexerGenerator (expecially the ones for Java) is that since it is optimized for performance we **don't return objects** and we **use the minimum possible of objects internally**.    
This actually is the main reason of the ~20x when compared with javacc.


Configuration File
------------------
Is a simple *key: value* configuration file plus the *specification of your grammar*.   
The four configuration keys are listed below:

    # LEXER GENERATOR configuration file
    # ---------------------------------------
    # Place *first* the generic configuration
    # then list your grammar.

    PACKAGE:          org.apache.asterix.admfast.parser
    LEXER_NAME:       AdmLexer
    OUTPUT_DIR:       output/


Specify The Grammar
-------------------
Your grammar has to be listed in the configuration file after the *TOKENS:* keyword.

    TOKENS:

    BOOLEAN_LIT        = string(boolean)
    COMMA              = char(\,)
    COLON              = char(:)
    STRING_LITERAL     = char("), anythingUntil(")
    INT_LITERAL        = signOrNothing(), digitSequence()
    INT8_LITERAL       = token(INT_LITERAL), string(i8)
    @EXPONENT          = caseInsensitiveChar(e), signOrNothing(), digitSequence()
    DOUBLE_LITERAL     = signOrNothing(), digitSequence(), char(.), digitSequence(), token(@EXPONENT)
    DOUBLE_LITERAL     = signOrNothing(), digitSequence(), token(@EXPONENT)

Each token is composed by a **name** and a sequence of **rules**.   
Each rule is then written with the format: **constructor(parameter)**  
the list of the rules available is coded inside *NodeChainFactory.java*

You can write more than a sequence of rules just addind more another line and repeating the token name.

You can reuse the rules of a token inside another one with the special rule: **token(** *TOKEN_NAME* **)**

Lastly you can define *auxiliary* token definitions that will not be encoded in the final lexer (but that can be useful inside other token definitions) just **startig the token name with @**.

**Attention:** please pay attention to not write rules that once merged int the state machine would lead to a *conflict between transaction* like a transaction for a generic digit and one for a the digit 0 from the same node. 

The result: MyLexer
-------------------
The result of the execution of the LexerGenerator is the creation of the Lexer inside the directory *components**.
The lexer is extremly easy and minimal and can be used likewise an Iterator:

     MyLexer myLexer = new MyLexer(new FileReader(file)));
     while((token = MyLexer.next()) != MyLexer.TOKEN_EOF){
        System.out.println(MyLexer.tokenKindToString(token));
     }

