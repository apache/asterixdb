/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package [PACKAGE]; 

import java.io.IOException;
import [PACKAGE].[LEXER_NAME]Exception;

public class [LEXER_NAME] {

    public static final int
        TOKEN_EOF = 0, TOKEN_AUX_NOT_FOUND = 1 [TOKENS_CONSTANTS];

    // Human representation of tokens. Useful for debug.
    // Is possible to convert a TOKEN_CONSTANT in its image through
    // [LEXER_NAME].tokenKindToString(TOKEN_CONSTANT); 
    private static final String[] tokenImage = {
            "<EOF>", "<AUX_NOT_FOUND>" [TOKENS_IMAGES]
          };
    
    private static final char EOF_CHAR = 4;
    protected java.io.Reader inputStream;
    protected int column;
    protected int line;
    protected boolean prevCharIsCR;
    protected boolean prevCharIsLF;
    protected char[] buffer;
    protected int bufsize;
    protected int bufpos;
    protected int tokenBegin;
    protected int endOf_USED_Buffer;
    protected int endOf_UNUSED_Buffer;
    protected int maxUnusedBufferSize;

// ================================================================================
//  Auxiliary functions. Can parse the tokens used in the grammar as partial/auxiliary
// ================================================================================

    [LEXER_AUXFUNCTIONS]

// ================================================================================
//  Main method. Return a TOKEN_CONSTANT
// ================================================================================            
            
    public int next() throws [LEXER_NAME]Exception, IOException{
        char currentChar = buffer[bufpos];
        while (currentChar == ' ' || currentChar=='\t' || currentChar == '\n' || currentChar=='\r')
            currentChar = readNextChar(); 
        tokenBegin = bufpos;
        if (currentChar==EOF_CHAR) return TOKEN_EOF;

        [LEXER_LOGIC]
    }

// ================================================================================
//  Public interface
// ================================================================================
    
    public [LEXER_NAME](java.io.Reader stream) throws IOException{
        reInit(stream);
    }

    public void reInit(java.io.Reader stream) throws IOException{
        done();
        inputStream    = stream;
        bufsize        = 4096;
        line           = 1;
        column         = 0;
        bufpos         = -1;
        endOf_UNUSED_Buffer = bufsize;
        endOf_USED_Buffer = 0;
        prevCharIsCR   = false;
        prevCharIsLF   = false;
        buffer         = new char[bufsize];
        tokenBegin     = -1;
        maxUnusedBufferSize = 4096/2;
        readNextChar();
    }

    public String getLastTokenImage() {
        if (bufpos >= tokenBegin)
            return new String(buffer, tokenBegin, bufpos - tokenBegin);
          else
            return new String(buffer, tokenBegin, bufsize - tokenBegin) +
                                  new String(buffer, 0, bufpos);
    }
    
    public static String tokenKindToString(int token) {
        return tokenImage[token]; 
    }

    public void done(){
        buffer = null;
    }

// ================================================================================
//  Parse error management
// ================================================================================    
    
    protected int parseError(String reason) throws [LEXER_NAME]Exception {
        StringBuilder message = new StringBuilder();
        message.append(reason).append("\n");
        message.append("Line: ").append(line).append("\n");
        message.append("Row: ").append(column).append("\n");
        throw new [LEXER_NAME]Exception(message.toString());
    }

    protected int parseError(int ... tokens) throws [LEXER_NAME]Exception {
        StringBuilder message = new StringBuilder();
        message.append("Error while parsing. ");
        message.append(" Line: ").append(line);
        message.append(" Row: ").append(column);
        message.append(" Expecting:");
        for (int tokenId : tokens){
            message.append(" ").append([LEXER_NAME].tokenKindToString(tokenId));
        }
        throw new [LEXER_NAME]Exception(message.toString());
    }
    
    protected void updateLineColumn(char c){
        column++;
    
        if (prevCharIsLF)
        {
            prevCharIsLF = false;
            line += (column = 1);
        }
        else if (prevCharIsCR)
        {
            prevCharIsCR = false;
            if (c == '\n')
            {
                prevCharIsLF = true;
            }
            else
            {
                line += (column = 1);
            }
        }
        
        if (c=='\r') {
            prevCharIsCR = true;
        } else if(c == '\n') {
            prevCharIsLF = true;
        }
    }
    
// ================================================================================
//  Read data, buffer management. It uses a circular (and expandable) buffer
// ================================================================================    

    protected char readNextChar() throws IOException {
        if (++bufpos >= endOf_USED_Buffer)
            fillBuff();
        char c = buffer[bufpos];
        updateLineColumn(c);
        return c;
    }

    protected boolean fillBuff() throws IOException {
        if (endOf_UNUSED_Buffer == endOf_USED_Buffer) // If no more unused buffer space 
        {
          if (endOf_UNUSED_Buffer == bufsize)         // -- If the previous unused space was
          {                                           // -- at the end of the buffer
            if (tokenBegin > maxUnusedBufferSize)     // -- -- If the first N bytes before
            {                                         //       the current token are enough
              bufpos = endOf_USED_Buffer = 0;         // -- -- -- setup buffer to use that fragment 
              endOf_UNUSED_Buffer = tokenBegin;
            }
            else if (tokenBegin < 0)                  // -- -- If no token yet
              bufpos = endOf_USED_Buffer = 0;         // -- -- -- reuse the whole buffer
            else
              ExpandBuff(false);                      // -- -- Otherwise expand buffer after its end
          }
          else if (endOf_UNUSED_Buffer > tokenBegin)  // If the endOf_UNUSED_Buffer is after the token
            endOf_UNUSED_Buffer = bufsize;            // -- set endOf_UNUSED_Buffer to the end of the buffer
          else if ((tokenBegin - endOf_UNUSED_Buffer) < maxUnusedBufferSize)
          {                                           // If between endOf_UNUSED_Buffer and the token
            ExpandBuff(true);                         // there is NOT enough space expand the buffer                          
          }                                           // reorganizing it
          else 
            endOf_UNUSED_Buffer = tokenBegin;         // Otherwise there is enough space at the start
        }                                             // so we set the buffer to use that fragment
        int i;
        if ((i = inputStream.read(buffer, endOf_USED_Buffer, endOf_UNUSED_Buffer - endOf_USED_Buffer)) == -1)
        {
            inputStream.close();
            buffer[endOf_USED_Buffer]=(char)EOF_CHAR;
            endOf_USED_Buffer++;
            return false;
        }
            else
                endOf_USED_Buffer += i;
        return true;
    }


    protected void ExpandBuff(boolean wrapAround)
    {
      char[] newbuffer = new char[bufsize + maxUnusedBufferSize];

      try {
        if (wrapAround) {
          System.arraycopy(buffer, tokenBegin, newbuffer, 0, bufsize - tokenBegin);
          System.arraycopy(buffer, 0, newbuffer, bufsize - tokenBegin, bufpos);
          buffer = newbuffer;
          endOf_USED_Buffer = (bufpos += (bufsize - tokenBegin));
        }
        else {
          System.arraycopy(buffer, tokenBegin, newbuffer, 0, bufsize - tokenBegin);
          buffer = newbuffer;
          endOf_USED_Buffer = (bufpos -= tokenBegin);
        }
      } catch (Throwable t) {
          throw new Error(t.getMessage());
      }

      bufsize += maxUnusedBufferSize;
      endOf_UNUSED_Buffer = bufsize;
      tokenBegin = 0;
    }    
}
