/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.classad;

import java.io.IOException;

import org.apache.asterix.external.classad.Value.NumberFactor;

public class Lexer {

    public static final char[] TRUE_CHAR_ARRAY = "true".toCharArray();
    public static final char[] FALSE_CHAR_ARRAY = "false".toCharArray();
    public static final char[] UNDEFINED_CHAR_ARRAY = "undefined".toCharArray();
    public static final char[] ERROR_CHAR_ARRAY = "error".toCharArray();
    public static final char[] IS_CHAR_ARRAY = "is".toCharArray();
    public static final char[] ISNT_CHAR_ARRAY = "isnt".toCharArray();

    public static final char EOF = (char) -1;
    // internal state of lexical analyzer
    protected boolean initialized;
    private TokenType tokenType; // the integer id of the token
    private LexerSource lexSource;
    private char ch; // the current character
    private boolean accumulating; // are we in a token?
    private final boolean debug = false; // debug flag
    // internal buffer for token accumulation
    private AMutableCharArrayString lexBuffer;

    // cached last token
    private TokenValue yylval; // the token itself
    private boolean tokenConsumed; // has the token been consumed?

    public enum TokenType {
        LEX_TOKEN_ERROR,
        LEX_END_OF_INPUT,
        LEX_TOKEN_TOO_LONG,
        LEX_INTEGER_VALUE,
        LEX_REAL_VALUE,
        LEX_BOOLEAN_VALUE,
        LEX_STRING_VALUE,
        LEX_UNDEFINED_VALUE,
        LEX_ERROR_VALUE,
        LEX_IDENTIFIER,
        LEX_SELECTION,
        LEX_MULTIPLY,
        LEX_DIVIDE,
        LEX_MODULUS,
        LEX_PLUS,
        LEX_MINUS,
        LEX_BITWISE_AND,
        LEX_BITWISE_OR,
        LEX_BITWISE_NOT,
        LEX_BITWISE_XOR,
        LEX_LEFT_SHIFT,
        LEX_RIGHT_SHIFT,
        LEX_URIGHT_SHIFT,
        LEX_LOGICAL_AND,
        LEX_LOGICAL_OR,
        LEX_LOGICAL_NOT,
        LEX_LESS_THAN,
        LEX_LESS_OR_EQUAL,
        LEX_GREATER_THAN,
        LEX_GREATER_OR_EQUAL,
        LEX_EQUAL,
        LEX_NOT_EQUAL,
        LEX_META_EQUAL,
        LEX_META_NOT_EQUAL,
        LEX_BOUND_TO,
        LEX_QMARK,
        LEX_COLON,
        LEX_COMMA,
        LEX_SEMICOLON,
        LEX_OPEN_BOX,
        LEX_CLOSE_BOX,
        LEX_OPEN_PAREN,
        LEX_CLOSE_PAREN,
        LEX_OPEN_BRACE,
        LEX_CLOSE_BRACE,
        LEX_BACKSLASH,
        LEX_ABSOLUTE_TIME_VALUE,
        LEX_RELATIVE_TIME_VALUE
    }

    public Lexer() {
        // initialize lexer state (token, etc.) variables
        tokenType = TokenType.LEX_END_OF_INPUT;
        ch = 0;
        tokenConsumed = true;
        accumulating = false;
        initialized = false;
        yylval = new TokenValue();
        return;
    }

    // Initialization method:  Initialize with immutable string
    //   +  Token will be accumulated in the lexBuffer
    public boolean initialize(LexerSource source) throws IOException {
        lexSource = source;
        ch = lexSource.readCharacter();
        // token state initialization
        if (lexBuffer != null) {
            lexBuffer.reset();
        } else {
            lexBuffer = new AMutableCharArrayString();
        }
        lexBuffer.setChar(0, ch);
        lexBuffer.setLength(0);
        //lexBufferCount = 0;
        tokenConsumed = true;
        accumulating = false;
        initialized = true;
        return true;
    }

    public boolean reinitialize() throws IOException {
        ch = lexSource.readCharacter();
        // token state initialization
        lexBuffer.setChar(0, ch);
        lexBuffer.setLength(0);
        tokenConsumed = true;
        accumulating = false;
        return true;
    }

    public boolean wasInitialized() {
        return initialized;
    }

    // FinishedParse:  This function implements the cleanup phase of a parse.
    //   String valued tokens are entered into a string space, and maintained
    //   with reference counting.  When a parse is finished, this space is flushed
    //   out.
    public void finishedParse() {
        accumulating = false;
        return;
    }

    // Mark:  This function is called when the beginning of a token is detected
    public void mark() {
        lexBuffer.setChar(0, ch);
        lexBuffer.setLength(1);
        accumulating = true;
        return;
    }

    // Cut:  This function is called when the end of a token is detected
    public void cut() {
        lexBuffer.decrementLength();
        accumulating = false;
        return;
    }

    // Wind:  This function is called when an additional character must be read
    //            from the input source; the conceptual action is to move the cursor
    public void wind() throws IOException {
        if (ch == (char) -1) {
            if (accumulating) {
                lexBuffer.incrementLength();
            }
            return;
        }
        ch = lexSource.readCharacter();
        if (ch == (char) -1) {
            if (accumulating) {
                lexBuffer.incrementLength();
            }
            return;
        }
        if (accumulating) {
            lexBuffer.appendChar(ch);
        }
    }

    public TokenType consumeToken() throws IOException {
        return consumeToken(null);
    }

    public TokenType consumeToken(TokenValue lvalp) throws IOException {
        if (lvalp != null) {
            lvalp.copyFrom(yylval);
        }
        // if a token has already been consumed, get another token
        if (tokenConsumed) {
            peekToken(lvalp);
        }
        if (debug) {
            System.out.printf("Consume: %s\n", strLexToken(tokenType));
        }

        tokenConsumed = true;
        return tokenType;
    }

    private boolean isxdigit(char ch) {
        return Character.isDigit(ch) || isLowerCaseHexaAlpha(ch) || isUppserCaseHexaAlpha(ch);
    }

    private boolean isUppserCaseHexaAlpha(char ch) {
        return ch >= 'a' && ch <= 'f';
    }

    private boolean isLowerCaseHexaAlpha(char ch2) {
        return ch >= 'A' && ch <= 'F';
    }

    public TokenType peekToken() throws IOException {
        return peekToken(null);
    }

    // peekToken() returns the same token till consumeToken() is called
    public TokenType peekToken(TokenValue lvalp) throws IOException {
        /*if (lvalp == null) {
            System.err.println("Null value passed to peekToken");
            return null;
        }*/
        if (!tokenConsumed) {

            if (lvalp != null) {
                lvalp.copyFrom(yylval);
            }
            return tokenType;
        }

        // Set the token to unconsumed
        tokenConsumed = false;

        // consume white space
        while (true) {
            if (Character.isWhitespace(ch)) {
                wind();
                continue;
            } else if (ch == '/') {
                mark();
                wind();
                if (ch == '/') {
                    // a c++ style comment
                    while (ch > 0 && ch != '\n') {
                        wind();
                    }
                } else if (ch == '*') {
                    // a c style comment
                    int oldCh;
                    ch = '\n';
                    do {
                        oldCh = ch;
                        wind();
                    } while ((oldCh != '*' || ch != '/') && (ch > 0));
                    if (ch == EOF) {
                        tokenType = TokenType.LEX_TOKEN_ERROR;
                        return (tokenType);
                    }
                    wind();
                } else {
                    // just a division operator
                    cut();
                    tokenType = TokenType.LEX_DIVIDE;
                    yylval.setTokenType(tokenType);
                    return (tokenType);
                }
            } else {
                break; // out of while( true ) loop
            }
        }

        // check if this is the end of the input
        if (ch == EOF) {
            tokenType = TokenType.LEX_END_OF_INPUT;
            yylval.setTokenType(tokenType);
            return tokenType;
        }

        // check the first character of the token
        if (ch == '-') {
            // Depending on the last token we saw, a minus may be the start
            // of an integer or real token. tokenizeNumber() does the right
            // thing if there is no subsequent integer or real.
            switch (tokenType) {
                case LEX_INTEGER_VALUE:
                case LEX_REAL_VALUE:
                case LEX_BOOLEAN_VALUE:
                case LEX_STRING_VALUE:
                case LEX_UNDEFINED_VALUE:
                case LEX_ERROR_VALUE:
                case LEX_IDENTIFIER:
                case LEX_SELECTION:
                case LEX_CLOSE_BOX:
                case LEX_CLOSE_PAREN:
                case LEX_CLOSE_BRACE:
                case LEX_BACKSLASH:
                case LEX_ABSOLUTE_TIME_VALUE:
                case LEX_RELATIVE_TIME_VALUE:
                    tokenizePunctOperator();
                    break;
                default:
                    tokenizeNumber();
                    break;
            }
        } else if (Character.isDigit(ch) || ch == '.') {
            // tokenizeNumber() also takes care of the selection operator
            tokenizeNumber();

        } else if (Character.isAlphabetic(ch) || ch == '_') {
            tokenizeAlphaHead();
        } else if (ch == '\"') {
            tokenizeString('\"'); // its a string literal
        } else if (ch == '\'') {
            tokenizeString('\''); // its a quoted attribute
        }

        else {
            tokenizePunctOperator();
        }

        if (debug) {
            System.out.printf("Peek: %s\n", strLexToken(tokenType));
            if (tokenType == TokenType.LEX_ERROR_VALUE) {
                System.out.println("Lexer problem");
            }
        }
        if (lvalp != null) {
            lvalp.copyFrom(yylval);
        }
        yylval.setTokenType(tokenType);
        return tokenType;
    }

    // Tokenize number constants:
    //   1.  Integers:  [-] 0[0-7]+ | 0[xX][0-9a-fA-F]+ | [0-9]+
    //   2.  Reals   :  [-] [0-9]*\.[0-9]* (e|E) [+-]? [0-9]+
    enum NumberType {
        NONE,
        INTEGER,
        REAL
    }

    public TokenType tokenizeNumber() throws IOException {
        NumberType numberType = NumberType.NONE;
        NumberFactor f;
        long integer = 0;
        double real = 0;
        int och;

        och = ch;
        mark();
        wind();

        if (och == '-') {
            // This may be a negative number or the unary minus operator
            // The subsequent two characters will tell us which.
            if (Character.isDigit(ch)) {
                // It looks like a negative number, keep reading.
                och = ch;
                wind();
            } else if (ch == '.') {
                // This could be a real number or an attribute reference
                // starting with dot. Look at the second character.
                int ch2 = lexSource.readCharacter();
                if (ch2 >= 0) {
                    lexSource.unreadCharacter();
                }
                if (!Character.isDigit(ch2)) {
                    // It's not a real number, return a minus token.
                    cut();
                    tokenType = TokenType.LEX_MINUS;
                    return tokenType;
                }
                // It looks like a negative real, keep reading.
            } else {
                // It's not a number, return a minus token.
                cut();
                tokenType = TokenType.LEX_MINUS;
                return tokenType;
            }
        }

        if (och == '0') {
            // number is octal, hex or real
            if (Character.toLowerCase(ch) == 'x') {
                // get hex digits only; parse hex-digit+
                numberType = NumberType.INTEGER;
                wind();
                if (!isxdigit(ch)) {
                    cut();
                    tokenType = TokenType.LEX_TOKEN_ERROR;
                    return (tokenType);
                }
                while (isxdigit(ch)) {
                    wind();
                }
            } else {
                // get octal or real
                numberType = NumberType.INTEGER;
                while (Character.isDigit(ch)) {
                    wind();
                    if (!isodigit(ch)) {
                        // not an octal number
                        numberType = NumberType.REAL;
                    }
                }
                if (ch == '.' || Character.toLowerCase(ch) == 'e') {
                    numberType = NumberType.REAL;
                } else if (numberType == NumberType.REAL) {
                    // non-octal digits, but not a real (no '.' or 'e')
                    // so, illegal octal constant
                    cut();
                    tokenType = TokenType.LEX_TOKEN_ERROR;
                    return (tokenType);
                }
            }
        } else if (Character.isDigit(och)) {
            // decimal or real; get digits
            while (Character.isDigit(ch)) {
                wind();
            }
            numberType = (ch == '.' || Character.toLowerCase(ch) == 'e') ? NumberType.REAL : NumberType.INTEGER;
        }

        if (och == '.' || ch == '.') {
            // fraction part of real or selection operator
            if (ch == '.') {
                wind();
            }
            if (Character.isDigit(ch)) {
                // real; get digits after decimal point
                numberType = NumberType.REAL;
                while (Character.isDigit(ch)) {
                    wind();
                }
            } else {
                if (numberType != NumberType.NONE) {
                    // initially like a number, but no digit following the '.'
                    cut();
                    tokenType = TokenType.LEX_TOKEN_ERROR;
                    return (tokenType);
                }
                // selection operator
                cut();
                tokenType = TokenType.LEX_SELECTION;
                return (tokenType);
            }
        }

        // if we are tokenizing a real, the (optional) exponent part is left
        //   i.e., [eE][+-]?[0-9]+
        if (numberType == NumberType.REAL && Character.toLowerCase(ch) == 'e') {
            wind();
            if (ch == '+' || ch == '-') {
                wind();
            }
            if (!Character.isDigit(ch)) {
                cut();
                tokenType = TokenType.LEX_TOKEN_ERROR;
                return (tokenType);
            }
            while (Character.isDigit(ch)) {
                wind();
            }
        }

        if (numberType == NumberType.INTEGER) {
            cut();
            integer = Long.parseLong(lexBuffer.toString());
        } else if (numberType == NumberType.REAL) {
            cut();
            real = Double.parseDouble(lexBuffer.toString());
        } else {
            /* If we've reached this point, we have a serious programming
             * error: tokenizeNumber should only be called if we are
             * lexing a number or a selection, and we didn't find a number
             * or a selection. This should really never happen, so we
             * bomb if it does. It should be reported as a bug.
             */
            throw new IOException("Should not reach here");
        }

        switch (Character.toUpperCase(ch)) {
            case 'B':
                f = NumberFactor.B_FACTOR;
                wind();
                break;
            case 'K':
                f = NumberFactor.K_FACTOR;
                wind();
                break;
            case 'M':
                f = NumberFactor.M_FACTOR;
                wind();
                break;
            case 'G':
                f = NumberFactor.G_FACTOR;
                wind();
                break;
            case 'T':
                f = NumberFactor.T_FACTOR;
                wind();
                break;
            default:
                f = NumberFactor.NO_FACTOR;
        }

        if (numberType == NumberType.INTEGER) {
            yylval.setIntValue(integer, f);
            yylval.setTokenType(TokenType.LEX_INTEGER_VALUE);
            tokenType = TokenType.LEX_INTEGER_VALUE;
        } else {
            yylval.setRealValue(real, f);
            yylval.setTokenType(TokenType.LEX_REAL_VALUE);
            tokenType = TokenType.LEX_REAL_VALUE;
        }

        return (tokenType);
    }

    public static boolean isodigit(char ch) {
        return ch >= '0' && ch <= '7';
    }

    // Tokenize alpha head: (character sequences beggining with an alphabet)
    //   1.  Reserved character sequences:  true, false, error, undefined
    //   2.  Identifier                  :  [a-zA-Z_][a-zA-Z0-9_]*
    public TokenType tokenizeAlphaHead() throws IOException {
        mark();
        while (Character.isAlphabetic(ch)) {
            wind();
        }
        if (Character.isDigit(ch) || ch == '_') {
            // The token is an identifier; consume the rest of the token
            wind();
            while (Character.isAlphabetic(ch) || Character.isDigit(ch) || ch == '_') {
                wind();
            }
            cut();
            tokenType = TokenType.LEX_IDENTIFIER;
            yylval.setStringValue(lexBuffer);
            return tokenType;
        }

        // check if the string is one of the reserved words; Case insensitive
        cut();
        if (isEqualIgnoreCase(TRUE_CHAR_ARRAY)) {
            tokenType = TokenType.LEX_BOOLEAN_VALUE;
            yylval.setBoolValue(true);
        } else if (isEqualIgnoreCase(FALSE_CHAR_ARRAY)) {
            tokenType = TokenType.LEX_BOOLEAN_VALUE;
            yylval.setBoolValue(false);
        } else if (isEqualIgnoreCase(UNDEFINED_CHAR_ARRAY)) {
            tokenType = TokenType.LEX_UNDEFINED_VALUE;
        } else if (isEqualIgnoreCase(ERROR_CHAR_ARRAY)) {
            tokenType = TokenType.LEX_ERROR_VALUE;
        } else if (isEqualIgnoreCase(IS_CHAR_ARRAY)) {
            tokenType = TokenType.LEX_META_EQUAL;
        } else if (isEqualIgnoreCase(ISNT_CHAR_ARRAY)) {
            tokenType = TokenType.LEX_META_NOT_EQUAL;
        } else {
            // token is a character only identifier
            tokenType = TokenType.LEX_IDENTIFIER;
            yylval.setStringValue(lexBuffer);
        }
        return tokenType;
    }

    private boolean isEqualIgnoreCase(char[] compareTo) {
        return lexBuffer.isEqualsIgnoreCaseLower(compareTo);
    }

    // tokenizeStringLiteral:  Scans strings of the form " ... " or '...'
    // based on whether the argument passed was '\"' or '\''
    public TokenType tokenizeString(char delim) throws IOException {
        boolean stringComplete = false;

        // need to mark() after the quote
        wind();
        mark();

        while (!stringComplete) {
            boolean oddBackWhacks = false;
            char oldCh = 0;
            // consume the string literal; read upto " ignoring \"
            while ((ch > 0) && (ch != delim || (ch == delim && oldCh == '\\' && oddBackWhacks))) {
                if (!oddBackWhacks && ch == '\\') {
                    oddBackWhacks = true;
                } else {
                    oddBackWhacks = false;
                }
                oldCh = ch;
                wind();
            }

            if (ch == delim) {
                char tempch = ' ';
                // read past the whitespace characters
                while (Character.isWhitespace(tempch)) {
                    tempch = lexSource.readCharacter();
                }
                if (tempch != delim) { // a new token exists after the string
                    if (tempch != EOF) {
                        lexSource.unreadCharacter();
                    }
                    stringComplete = true;
                } else { // the adjacent string is to be concatenated to the existing string
                    lexBuffer.erase(lexBuffer.getLength());// erase the lagging '\"'
                    wind();
                }
            } else {
                // loop quit due to ch == 0 or ch == EOF
                tokenType = TokenType.LEX_TOKEN_ERROR;
                return tokenType;
            }
        }
        cut();
        wind(); // skip over the close quote
        boolean validStr = true; // to check if string is valid after converting escape
        validStr = Util.convertEscapes(lexBuffer);
        yylval.setStringValue(lexBuffer);
        if (validStr) {
            if (delim == '\"') {
                tokenType = TokenType.LEX_STRING_VALUE;
            } else {
                tokenType = TokenType.LEX_IDENTIFIER;
            }
        } else {
            tokenType = TokenType.LEX_TOKEN_ERROR; // string conatins a '\0' character inbetween
        }

        return tokenType;
    }

    // tokenizePunctOperator:  Tokenize puncutation and operators
    public TokenType tokenizePunctOperator() throws IOException {
        // save character; may need to lookahead
        char oldch = ch;
        char extra_lookahead;

        mark();
        wind();
        switch (oldch) {
            // these cases don't need lookaheads
            case '.':
                tokenType = TokenType.LEX_SELECTION;
                break;

            case '*':
                tokenType = TokenType.LEX_MULTIPLY;
                break;

            case '/':
                tokenType = TokenType.LEX_DIVIDE;
                break;

            case '%':
                tokenType = TokenType.LEX_MODULUS;
                break;

            case '+':
                tokenType = TokenType.LEX_PLUS;
                break;

            case '-':
                tokenType = TokenType.LEX_MINUS;
                break;

            case '~':
                tokenType = TokenType.LEX_BITWISE_NOT;
                break;

            case '^':
                tokenType = TokenType.LEX_BITWISE_XOR;
                break;

            case '?':
                tokenType = TokenType.LEX_QMARK;
                break;

            case ':':
                tokenType = TokenType.LEX_COLON;
                break;

            case ';':
                tokenType = TokenType.LEX_SEMICOLON;
                break;

            case ',':
                tokenType = TokenType.LEX_COMMA;
                break;

            case '[':
                tokenType = TokenType.LEX_OPEN_BOX;
                break;

            case ']':
                tokenType = TokenType.LEX_CLOSE_BOX;
                break;

            case '(':
                tokenType = TokenType.LEX_OPEN_PAREN;
                break;

            case ')':
                tokenType = TokenType.LEX_CLOSE_PAREN;
                break;

            case '{':
                tokenType = TokenType.LEX_OPEN_BRACE;
                break;

            case '}':
                tokenType = TokenType.LEX_CLOSE_BRACE;
                break;

            // the following cases need lookaheads

            case '&':
                tokenType = TokenType.LEX_BITWISE_AND;
                if (ch == '&') {
                    tokenType = TokenType.LEX_LOGICAL_AND;
                    wind();
                }
                break;

            case '|':
                tokenType = TokenType.LEX_BITWISE_OR;
                if (ch == '|') {
                    tokenType = TokenType.LEX_LOGICAL_OR;
                    wind();
                }
                break;

            case '<':
                tokenType = TokenType.LEX_LESS_THAN;
                switch (ch) {
                    case '=':
                        tokenType = TokenType.LEX_LESS_OR_EQUAL;
                        wind();
                        break;

                    case '<':
                        tokenType = TokenType.LEX_LEFT_SHIFT;
                        wind();
                        break;

                    default:
                        // just the '<' --- no need to do anything
                        break;
                }
                break;

            case '>':
                tokenType = TokenType.LEX_GREATER_THAN;
                switch (ch) {
                    case '=':
                        tokenType = TokenType.LEX_GREATER_OR_EQUAL;
                        wind();
                        break;

                    case '>':
                        tokenType = TokenType.LEX_RIGHT_SHIFT;
                        wind();
                        if (ch == '>') {
                            tokenType = TokenType.LEX_URIGHT_SHIFT;
                            wind();
                        }
                        break;

                    default:
                        // just the '>' --- no need to do anything
                        break;
                }
                break;

            case '=':
                tokenType = TokenType.LEX_BOUND_TO;
                switch (ch) {
                    case '=':
                        tokenType = TokenType.LEX_EQUAL;
                        wind();
                        break;

                    case '?':
                        tokenType = TokenType.LEX_META_EQUAL;
                        wind();

                        // ensure the trailing '=' of the '=?=' combination
                        if (ch != '=') {
                            tokenType = TokenType.LEX_TOKEN_ERROR;
                            return tokenType;
                        }

                        wind();
                        break;

                    case '!':
                        extra_lookahead = lexSource.readCharacter();
                        lexSource.unreadCharacter();
                        if (extra_lookahead == '=') {
                            tokenType = TokenType.LEX_META_NOT_EQUAL;
                            wind();
                            wind();
                        }
                        break;

                    default:
                        // just the '=' --- no need to do anything
                        break;
                }
                break;

            case '!':
                tokenType = TokenType.LEX_LOGICAL_NOT;
                switch (ch) {
                    case '=':
                        tokenType = TokenType.LEX_NOT_EQUAL;
                        wind();
                        break;

                    default:
                        // just the '!' --- no need to do anything
                        break;
                }
                break;

            default:
                tokenType = TokenType.LEX_TOKEN_ERROR;
                return tokenType;
        }

        // cut the token and return
        cut();
        return tokenType;
    }

    // strLexToken:  Return string representation of token type
    public static String strLexToken(TokenType tokenType) {
        switch (tokenType) {
            case LEX_END_OF_INPUT:
                return "LEX_END_OF_INPUT";
            case LEX_TOKEN_ERROR:
                return "LEX_TOKEN_ERROR";
            case LEX_TOKEN_TOO_LONG:
                return "LEX_TOKEN_TOO_LONG";

            case LEX_INTEGER_VALUE:
                return "LEX_INTEGER_VALUE";
            case LEX_REAL_VALUE:
                return "LEX_REAL_VALUE";
            case LEX_BOOLEAN_VALUE:
                return "LEX_BOOLEAN_VALUE";
            case LEX_STRING_VALUE:
                return "LEX_STRING_VALUE";
            case LEX_UNDEFINED_VALUE:
                return "LEX_UNDEFINED_VALUE";
            case LEX_ERROR_VALUE:
                return "LEX_ERROR_VALUE";

            case LEX_IDENTIFIER:
                return "LEX_IDENTIFIER";
            case LEX_SELECTION:
                return "LEX_SELECTION";

            case LEX_MULTIPLY:
                return "LEX_MULTIPLY";
            case LEX_DIVIDE:
                return "LEX_DIVIDE";
            case LEX_MODULUS:
                return "LEX_MODULUS";
            case LEX_PLUS:
                return "LEX_PLUS";
            case LEX_MINUS:
                return "LEX_MINUS";

            case LEX_BITWISE_AND:
                return "LEX_BITWISE_AND";
            case LEX_BITWISE_OR:
                return "LEX_BITWISE_OR";
            case LEX_BITWISE_NOT:
                return "LEX_BITWISE_NOT";
            case LEX_BITWISE_XOR:
                return "LEX_BITWISE_XOR";

            case LEX_LEFT_SHIFT:
                return "LEX_LEFT_SHIFT";
            case LEX_RIGHT_SHIFT:
                return "LEX_RIGHT_SHIFT";
            case LEX_URIGHT_SHIFT:
                return "LEX_URIGHT_SHIFT";

            case LEX_LOGICAL_AND:
                return "LEX_LOGICAL_AND";
            case LEX_LOGICAL_OR:
                return "LEX_LOGICAL_OR";
            case LEX_LOGICAL_NOT:
                return "LEX_LOGICAL_NOT";

            case LEX_LESS_THAN:
                return "LEX_LESS_THAN";
            case LEX_LESS_OR_EQUAL:
                return "LEX_LESS_OR_EQUAL";
            case LEX_GREATER_THAN:
                return "LEX_GREATER_THAN";
            case LEX_GREATER_OR_EQUAL:
                return "LEX_GREATER_OR_EQUAL";
            case LEX_EQUAL:
                return "LEX_EQUAL";
            case LEX_NOT_EQUAL:
                return "LEX_NOT_EQUAL";
            case LEX_META_EQUAL:
                return "LEX_META_EQUAL";
            case LEX_META_NOT_EQUAL:
                return "LEX_META_NOT_EQUAL";

            case LEX_BOUND_TO:
                return "LEX_BOUND_TO";

            case LEX_QMARK:
                return "LEX_QMARK";
            case LEX_COLON:
                return "LEX_COLON";
            case LEX_SEMICOLON:
                return "LEX_SEMICOLON";
            case LEX_COMMA:
                return "LEX_COMMA";
            case LEX_OPEN_BOX:
                return "LEX_OPEN_BOX";
            case LEX_CLOSE_BOX:
                return "LEX_CLOSE_BOX";
            case LEX_OPEN_PAREN:
                return "LEX_OPEN_PAREN";
            case LEX_CLOSE_PAREN:
                return "LEX_CLOSE_PAREN";
            case LEX_OPEN_BRACE:
                return "LEX_OPEN_BRACE";
            case LEX_CLOSE_BRACE:
                return "LEX_CLOSE_BRACE";
            case LEX_BACKSLASH:
                return "LEX_BACKSLASH";
            case LEX_ABSOLUTE_TIME_VALUE:
                return "LEX_ABSOLUTE_TIME_VALUE";
            case LEX_RELATIVE_TIME_VALUE:
                return "LEX_RELATIVE_TIME_VALUE";

            default:
                return "** Unknown **";
        }
    }

    public LexerSource getLexSource() {
        return lexSource;
    }
}
