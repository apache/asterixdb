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
package org.apache.asterix.external.classad.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.asterix.external.classad.AMutableCharArrayString;
import org.apache.asterix.external.classad.ClassAd;
import org.apache.asterix.external.classad.ClassAdUnParser;
import org.apache.asterix.external.classad.ExprList;
import org.apache.asterix.external.classad.ExprTree;
import org.apache.asterix.external.classad.ExprTree.NodeKind;
import org.apache.asterix.external.classad.ExprTreeHolder;
import org.apache.asterix.external.classad.Lexer.TokenType;
import org.apache.asterix.external.classad.PrettyPrint;
import org.apache.asterix.external.classad.StringLexerSource;
import org.apache.asterix.external.classad.Value;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.external.library.ClassAdParser;
import org.apache.asterix.om.base.AMutableString;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FunctionalTester {

    public static Map<String, Variable> variables = new HashMap<String, FunctionalTester.Variable>();
    public static boolean haveCachedLine = false;
    public static String cachedLine = "";

    public static class Variable {
        public String name;
        public boolean isTree; // If false, then is value
        public ExprTreeHolder tree;
        public Value value;

        public Variable(String name, ExprTree tree, ClassAdObjectPool objectPool) {
            this.name = name;
            this.tree = new ExprTreeHolder(tree, objectPool);
            this.isTree = true;
        }

        public Variable(ClassAdObjectPool objectPool) {
            this.name = null;
            this.tree = new ExprTreeHolder(objectPool);
            this.isTree = false;
        }

        public Variable(String name, Value value) {
            this.name = name;
            this.value = value;
            this.isTree = false;
            this.tree = null;
        }

        public void getStringRepresentation(AMutableCharArrayString representation, ClassAdObjectPool objectPool)
                throws HyracksDataException {
            ClassAdUnParser unparser = new PrettyPrint(objectPool);

            if (isTree) {
                unparser.unparse(representation, tree);
            } else {
                unparser.unparse(representation, value);
            }
            return;
        }

    }

    /*--------------------------------------------------------------------
     *
     * Private Data Types
     *
     *--------------------------------------------------------------------*/

    public enum Command {
        cmd_NoCommand,
        cmd_InvalidCommand,
        cmd_Let,
        cmd_Eval,
        cmd_Print,
        cmd_Same,
        cmd_Sameq,
        cmd_Diff,
        cmd_Diffq,
        cmd_Set,
        cmd_Show,
        cmd_Writexml,
        cmd_Readxml,
        cmd_Echo,
        cmd_Help,
        cmd_Quit
    }

    public enum PrintFormat {
        print_Compact,
        print_Pretty,
        print_XML,
        print_XMLPretty
    }

    public static class Parameters {
        public boolean debug;
        public boolean verbose;
        public boolean interactive;
        public BufferedReader inputFile;

        public Parameters() {
            inputFile = null;
        }

        /*********************************************************************
         * Function: Parameters::ParseCommandLine
         * Purpose: This parses the command line. Note that it will exit
         * if there are any problems.
         *
         * @throws IOException
         *********************************************************************/
        public void parseCommandLine(int argc, String[] argv) throws IOException {
            // First we set up the defaults.
            debug = false;
            verbose = false;
            interactive = true;
            inputFile = null;

            // Then we parse to see what the user wants.
            for (int argIndex = 1; argIndex < argc; argIndex++) {
                if (argv[argIndex].equalsIgnoreCase("-d") || argv[argIndex].equalsIgnoreCase("-debug")) {
                    debug = false;
                } else if (argv[argIndex].equalsIgnoreCase("-v") || argv[argIndex].equalsIgnoreCase("-verbose")) {
                    verbose = true;
                } else {
                    if (inputFile == null) {
                        interactive = false;
                        inputFile = Files.newBufferedReader(Paths.get(argv[argIndex]), StandardCharsets.UTF_8);
                    }
                }
            }
            return;
        }
    }

    public static class State {
        public int number_of_errors;
        public int lineNumber;
        public PrintFormat format;

        public State() {
            number_of_errors = 0;
            lineNumber = 0;
            format = PrintFormat.print_Compact;
        }
    }

    // typedef map<string, Variable *> VariableMap;

    /*--------------------------------------------------------------------
     *
     * Private Functions
     *
     *--------------------------------------------------------------------*/

    public static int test(int argc, String[] argv, ClassAdObjectPool objectPool) throws IOException {
        // here
        boolean quit;
        AMutableString line = new AMutableString(null);
        State state = new State();
        Parameters parameters = new Parameters();

        print_version();
        parameters.parseCommandLine(argc, argv);
        quit = false;

        while (!quit && readLine(line, state, parameters) == true) {
            boolean good_line;
            Command command;

            good_line = replace_variables(line, state, parameters, objectPool);
            if (good_line) {
                command = get_command(line, parameters);
                quit = handle_command(command, line, state, parameters, objectPool);
            }
        }
        print_final_state(state);

        if (!parameters.interactive && parameters.inputFile != null) {
            parameters.inputFile.close();
        }

        if (state.number_of_errors == 0) {
            return 0;
        } else {
            return 1;
        }
    }

    /*********************************************************************
     * Function: read_line
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static boolean readLine(AMutableString line, State state, Parameters parameters) throws IOException {
        boolean haveInput;

        if (parameters.interactive) {
            haveInput = read_line_stdin(line, state, parameters);
        } else {
            haveInput = read_line_file(line, state, parameters);
        }
        return haveInput;
    }

    /*********************************************************************
     * Function: read_line_stdin
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static boolean read_line_stdin(AMutableString line, State state, Parameters parameters) throws IOException {
        System.out.print("> ");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line_segment = br.readLine();
        if (line_segment.length() == 0 || line_segment.equalsIgnoreCase("q")) {
            line.setValue("");
            return false;
        }
        if (line_is_comment(line_segment)) {
            // ignore comments, should we read another line?
            line.setValue("");
            return true;
        } else {
            line.setValue(line.getStringValue() + line_segment);
            state.lineNumber++;
            return true;
        }
    }

    /*********************************************************************
     * Function: read_line_file
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static boolean read_line_file(AMutableString line, State state, Parameters parameters) throws IOException {
        boolean have_input;

        // We read a line, either from our one-line cache, or the file.
        if (!haveCachedLine) {
            cachedLine = parameters.inputFile.readLine();
            state.lineNumber++;
            haveCachedLine = true;
        } else {
            // We have a cached-line, but we need to increment the line number for it.
            // We don't increment it until we use it.
            state.lineNumber++;
        }
        if (cachedLine == null) {
            cachedLine = parameters.inputFile.readLine();
            if (cachedLine == null) {
                have_input = false;
                haveCachedLine = false;
            } else {
                line.setValue(cachedLine);
                have_input = true;
                haveCachedLine = false;
                cachedLine = null;
            }
        } else {
            line.setValue(cachedLine);
            have_input = true;
            haveCachedLine = false;
        }

        // If we actually have a non-comment line, we read another line
        // from the file If it begins with a whitespace character, then we
        // append it to the previous line, otherwise we cache it for the
        // next time we call this function.
        if (have_input) {
            if (line_is_comment(line.getStringValue())) {
                line.setValue("");
            } else {
                boolean done = false;
                while (!done) {
                    cachedLine = parameters.inputFile.readLine();
                    if (cachedLine != null && line_is_comment(cachedLine)) {
                        // ignore comments
                        state.lineNumber++;
                    } else if (cachedLine != null && cachedLine.length() == 0) {
                        line.setValue(line.getStringValue() + " ");
                        state.lineNumber++;
                    } else if (cachedLine != null && Character.isWhitespace(cachedLine.charAt(0))) {
                        line.setValue(line.getStringValue() + cachedLine);
                        state.lineNumber++;
                    } else {
                        done = true;
                        haveCachedLine = cachedLine != null ? true : false;
                    }
                }
            }
        }
        return have_input;
    }

    /*********************************************************************
     * Function: replace_variables
     * Purpose:
     *
     * @throws HyracksDataException
     *********************************************************************/
    public static boolean replace_variables(AMutableString mutableLine, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws HyracksDataException {
        boolean good_line;
        String error;

        good_line = true;
        error = "";
        Variable var = new Variable(objectPool);
        for (;;) {
            int dollar;
            int current_position;
            String variable_name;
            AMutableCharArrayString variable_value = new AMutableCharArrayString();
            current_position = 0;
            dollar = mutableLine.getStringValue().indexOf('$', current_position);
            if (dollar < 0) {
                break;
            }
            current_position = dollar + 1;
            if (!Character.isAlphabetic(mutableLine.getStringValue().charAt(current_position))) {
                good_line = false;
                error = "Bad variable name.";
                break;
            }
            current_position++;
            while (Character.isLetterOrDigit((mutableLine.getStringValue().charAt(current_position)))
                    || mutableLine.getStringValue().charAt(current_position) == '_') {
                current_position++;
            }

            variable_name = mutableLine.getStringValue().substring(dollar + 1, current_position);
            var = variables.get(variable_name);
            if (var == null) {
                good_line = false;
                error = "Unknown variable '$";
                error += variable_name;
                error += "'";
                break;
            }
            var.getStringRepresentation(variable_value, objectPool);

            // We have to be careful with substr() because with gcc 2.96, it likes to
            // assert/except if you give it values that are too large.
            String end;
            if (current_position < mutableLine.getStringValue().length()) {
                end = mutableLine.getStringValue().substring(current_position);
            } else {
                end = "";
            }
            mutableLine.setValue(mutableLine.getStringValue().substring(0, dollar) + variable_value.toString() + end);
        }

        if (parameters.debug) {
            System.err.println("# after replacement: " + mutableLine.getStringValue());
        }

        if (!good_line) {
            print_error_message(error, state);
        }
        return good_line;
    }

    /*********************************************************************
     * Function: get_command
     * Purpose:
     *********************************************************************/
    public static Command get_command(AMutableString line, Parameters parameters) {
        int current_position;
        int length;
        String command_name;
        Command command;

        current_position = 0;
        length = line.getStringValue().length();
        command_name = "";
        command = Command.cmd_NoCommand;

        // Skip whitespace
        while (current_position < length && Character.isWhitespace(line.getStringValue().charAt(current_position))) {
            current_position++;
        }
        // Find command name
        while (current_position < length && Character.isAlphabetic(line.getStringValue().charAt(current_position))) {
            command_name += line.getStringValue().charAt(current_position);
            current_position++;
        }
        // Figure out what the command is.
        if (command_name.length() == 0) {
            command = Command.cmd_NoCommand;
        } else if (command_name.equalsIgnoreCase("let")) {
            command = Command.cmd_Let;
        } else if (command_name.equalsIgnoreCase("eval")) {
            command = Command.cmd_Eval;
        } else if (command_name.equalsIgnoreCase("print")) {
            command = Command.cmd_Print;
        } else if (command_name.equalsIgnoreCase("same")) {
            command = Command.cmd_Same;
        } else if (command_name.equalsIgnoreCase("sameq")) {
            command = Command.cmd_Sameq;
        } else if (command_name.equalsIgnoreCase("diff")) {
            command = Command.cmd_Diff;
        } else if (command_name.equalsIgnoreCase("diffq")) {
            command = Command.cmd_Diffq;
        } else if (command_name.equalsIgnoreCase("set")) {
            command = Command.cmd_Set;
        } else if (command_name.equalsIgnoreCase("show")) {
            command = Command.cmd_Show;
        } else if (command_name.equalsIgnoreCase("writexml")) {
            command = Command.cmd_Writexml;
        } else if (command_name.equalsIgnoreCase("readxml")) {
            command = Command.cmd_Readxml;
        } else if (command_name.equalsIgnoreCase("echo")) {
            command = Command.cmd_Echo;
        } else if (command_name.equalsIgnoreCase("help")) {
            command = Command.cmd_Help;
        } else if (command_name.equalsIgnoreCase("quit")) {
            command = Command.cmd_Quit;
        } else {
            command = Command.cmd_InvalidCommand;
        }
        shorten_line(line, current_position);
        return command;
    }

    /*********************************************************************
     * Function: handle_command
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static boolean handle_command(Command command, AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws IOException {
        boolean quit = false;

        switch (command) {
            case cmd_NoCommand:
                // Ignore. This isn't a problem.
                break;
            case cmd_InvalidCommand:
                print_error_message("Unknown command on line", state);
                break;
            case cmd_Let:
                handle_let(line, state, parameters, objectPool);
                break;
            case cmd_Eval:
                handle_eval(line, state, parameters, objectPool);
                break;
            case cmd_Print:
                handle_print(line, state, parameters, objectPool);
                break;
            case cmd_Same:
                handle_same(line, state, parameters, objectPool);
                break;
            case cmd_Sameq:
                handle_sameq(line, state, parameters, objectPool);
                break;
            case cmd_Diff:
                handle_diff(line, state, parameters, objectPool);
                break;
            case cmd_Diffq:
                handle_diffq(line, state, parameters, objectPool);
                break;
            case cmd_Set:
                handle_set(line, state, parameters, objectPool);
                break;
            case cmd_Show:
                handle_show(line, state, parameters, objectPool);
                break;
            case cmd_Writexml:
                // handle_writexml(line, state, parameters);
                break;
            case cmd_Readxml:
                // handle_readxml(line, state, parameters);
                break;
            case cmd_Echo:
                handle_echo(line.getStringValue(), state, parameters);
                break;
            case cmd_Help:
                handle_help();
                break;
            case cmd_Quit:
                quit = true;
                break;
        }
        return quit;
    }

    /*********************************************************************
     * Function: handle_let
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static void handle_let(AMutableString line, State state, Parameters parameters, ClassAdObjectPool objectPool)
            throws IOException {
        AMutableString variable_name = new AMutableString(null);
        ExprTree tree;
        Variable variable;

        if (get_variable_name(line, true, variable_name, state, parameters)) {
            tree = get_expr(line, state, parameters, objectPool);
            if (tree != null) {
                variable = new Variable(variable_name.getStringValue(), tree, objectPool);
                variables.put(variable_name.getStringValue(), variable);
                if (parameters.interactive) {
                    print_expr(tree, state, parameters, objectPool);
                }
            }
        }
        return;
    }

    /*********************************************************************
     * Function: handle_eval
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static void handle_eval(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws IOException {
        AMutableString variable_name = new AMutableString("");
        ExprTree tree;
        Variable variable;

        if (get_variable_name(line, true, variable_name, state, parameters)) {
            tree = get_expr(line, state, parameters, objectPool);
            if (tree != null) {
                Value value = new Value(objectPool);
                if (!evaluate_expr(tree, value, parameters, objectPool)) {
                    print_error_message("Couldn't evaluate rvalue", state);
                } else {
                    variable = new Variable(variable_name.getStringValue(), value);
                    variables.put(variable_name.getStringValue(), variable);
                }
            }
        }
        return;
    }

    /*********************************************************************
     * Function: handle_print
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static void handle_print(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws IOException {
        ExprTree tree;
        tree = get_expr(line, state, parameters, objectPool);
        if (tree != null) {
            print_expr(tree, state, parameters, objectPool);
        }
    }

    /*********************************************************************
     * Function: handle_same
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static void handle_same(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws IOException {
        ExprTreeHolder tree = new ExprTreeHolder(objectPool);
        ExprTreeHolder tree2 = new ExprTreeHolder(objectPool);
        Value value1 = new Value(objectPool);
        Value value2 = new Value(objectPool);
        try {
            get_two_exprs(line, tree, tree2, state, parameters, objectPool);
            if (tree.getInnerTree() != null || tree2.getInnerTree() != null) {

                if (parameters.debug) {
                    System.out.println("Sameeval has two trees:");
                    System.out.print(" ");
                    print_expr(tree, state, parameters, objectPool);
                    System.out.print(" ");
                    print_expr(tree2, state, parameters, objectPool);
                }
                if (!evaluate_expr(tree, value1, parameters, objectPool)) {
                    print_error_message("Couldn't evaluate first expression.\n", state);
                } else if (!evaluate_expr(tree2, value2, parameters, objectPool)) {
                    print_error_message("Couldn't evaluate second expressions.\n", state);
                } else if (!value1.sameAs(value2)) {
                    print_error_message("the expressions are different.", state);
                    assert (false);
                }
                if (parameters.debug) {
                    System.out.println("They evaluated to: ");
                    System.out.println(" " + value1);
                    System.out.println(" " + value2);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assert (false);
        }
        return;
    }

    /*********************************************************************
     * Function: handle_sameq
     * Purpose:
     * @param objectPool 
     *
     * @throws IOException
     *********************************************************************/
    public static void handle_sameq(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws IOException {
        ExprTreeHolder tree = new ExprTreeHolder(objectPool);
        ExprTreeHolder tree2 = new ExprTreeHolder(objectPool);

        get_two_exprs(line, tree, tree2, state, parameters, objectPool);
        if (tree.getInnerTree() != null || tree2.getInnerTree() != null) {
            if (!tree.sameAs(tree2)) {
                print_error_message("the expressions are different.", state);
            }
        }
        return;
    }

    /*********************************************************************
     * Function: handle_diff
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static void handle_diff(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws IOException {
        ExprTreeHolder tree = new ExprTreeHolder(objectPool);
        ExprTreeHolder tree2 = new ExprTreeHolder(objectPool);
        Value value1 = new Value(objectPool);
        Value value2 = new Value(objectPool);

        get_two_exprs(line, tree, tree2, state, parameters, objectPool);
        if (tree.getInnerTree() != null || tree2.getInnerTree() != null) {
            if (!evaluate_expr(tree, value1, parameters, objectPool)) {
                print_error_message("Couldn't evaluate first expression.\n", state);
            } else if (!evaluate_expr(tree2, value2, parameters, objectPool)) {
                print_error_message("Couldn't evaluate second expressions.\n", state);
            } else if (value1.sameAs(value2)) {
                print_error_message("the expressions are the same.", state);
                assert (false);
            }
        }
        return;
    }

    /*********************************************************************
     * Function: handle_diffq
     * Purpose:
     * @param objectPool 
     *
     * @throws IOException
     *********************************************************************/
    public static void handle_diffq(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws IOException {
        ExprTreeHolder tree = new ExprTreeHolder(objectPool);
        ExprTreeHolder tree2 = new ExprTreeHolder(objectPool);

        get_two_exprs(line, tree, tree2, state, parameters, objectPool);
        if (tree.getInnerTree() != null || tree2.getInnerTree() != null) {
            if (tree.sameAs(tree2)) {
                print_error_message("the expressions are the same.", state);
            }
        }
        return;
    }

    /*********************************************************************
     * Function: handle_set
     * Purpose:
     * @param objectPool 
     *********************************************************************/
    public static void handle_set(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) {
        AMutableString option_name = new AMutableString(null);
        AMutableString option_value = new AMutableString(null);

        if (get_variable_name(line, false, option_name, state, parameters)) {
            if (get_variable_name(line, false, option_value, state, parameters)) {
                if (option_name.getStringValue().equalsIgnoreCase("format")) {
                    if (option_value.getStringValue().equalsIgnoreCase("compact")) {
                        state.format = PrintFormat.print_Compact;
                    } else if (option_value.getStringValue().equalsIgnoreCase("pretty")) {
                        state.format = PrintFormat.print_Pretty;
                    } else if (option_value.getStringValue().equalsIgnoreCase("xml")) {
                        state.format = PrintFormat.print_XML;
                    } else if (option_value.getStringValue().equalsIgnoreCase("xmlpretty")) {
                        state.format = PrintFormat.print_XMLPretty;
                    } else {
                        print_error_message("Unknown print format. Use compact, pretty, xml, or xmlpretty", state);
                    }
                } else {
                    print_error_message("Unknown option. The only option currently available is format", state);
                }
            }
        }
        return;
    }

    /*********************************************************************
     * Function: handle_show
     * Purpose:
     * @param objectPool 
     *********************************************************************/
    public static void handle_show(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) {
        AMutableString option_name = new AMutableString(null);

        if (get_variable_name(line, false, option_name, state, parameters)) {
            if (option_name.getStringValue().equalsIgnoreCase("format")) {
                System.out.print("Format: ");
                switch (state.format) {
                    case print_Compact:
                        System.out.print("Traditional Compact\n");
                        break;
                    case print_Pretty:
                        System.out.print("Traditional Pretty\n");
                        break;
                    case print_XML:
                        System.out.print("XML Compact\n");
                        break;
                    case print_XMLPretty:
                        System.out.print("XML Pretty\n");
                        break;
                }
            } else {
                print_error_message("Unknown option. The only option currently available is format", state);
            }
        }

        return;
    }

    /*********************************************************************
     * Function: handle_echo
     * Purpose:
     *********************************************************************/
    public static void handle_echo(String line, State state, Parameters parameters) {
        AMutableCharArrayString new_line = new AMutableCharArrayString();
        int index;

        index = 0;

        while (index < line.length() && Character.isWhitespace(line.charAt(index))) {
            index++;
        }
        while (index < line.length()) {
            new_line.appendChar(line.charAt(index));
            index++;
        }

        System.out.println(new_line.toString());
    }

    /*********************************************************************
     * Function: handle_help
     * Purpose:
     *********************************************************************/
    public static void handle_help() {
        print_version();

        System.out.println();
        System.out.println("Commands:");
        System.out.println("let name = expr   Set a variable to an unevaluated expression.");
        System.out.println("eval name = expr  Set a variable to an evaluated expression.");
        System.out.println("same expr1 expr2  Prints a message only if expr1 and expr2 are different.");
        System.out.println("sameq expr1 expr2 Prints a message only if expr1 and expr2 are different.");
        System.out.println("                   same evaluates its expressions first, sameq doesn't.");
        System.out.println("diff expr1 expr2  Prints a message only if expr1 and expr2 are the same.");
        System.out.println("diffq expr1 expr2 Prints a message only if expr1 and expr2 are the same.");
        System.out.println("                   diff evaluates its expressions first, diffq doesn't.");
        System.out.println("set opt value     Sets an option to a particular value.");
        System.out.println("quit              Exit this program.");
        System.out.println("help              Print this message.");
        System.out.println();
        System.out.println("Options (for the set command):");
        System.out.println("format              Set the way ClassAds print.");
        System.out.println("  compact           A compact, traditional style");
        System.out.println("  pretty            Traditional, with more spaces");
        System.out.println("  xml               A compact XML representation");
        System.out.println("  xmlpretty         XML with extra spacing for readability.");
        return;
    }

    /*********************************************************************
     * Function: get_variable_name
     * Purpose:
     *********************************************************************/
    public static boolean get_variable_name(AMutableString line, boolean swallow_equals, AMutableString variable_name,
            State state, Parameters parameters) {
        int current_position;
        int length;
        boolean have_good_name;

        current_position = 0;
        length = line.getStringValue().length();
        variable_name.setValue("");
        have_good_name = false;

        // Skip whitespace
        while (current_position < length && Character.isWhitespace(line.getStringValue().charAt(current_position))) {
            current_position++;
        }
        // Find variable name
        if (current_position < length && Character.isAlphabetic(line.getStringValue().charAt(current_position))) {
            variable_name.setValue(variable_name.getStringValue() + line.getStringValue().charAt(current_position));
            current_position++;
            // As soon as we have at least one character in the name, it's good.
            have_good_name = true;

            while (current_position < length
                    && (Character.isLetterOrDigit(line.getStringValue().charAt(current_position))
                            || line.getStringValue().charAt(current_position) == '_')) {
                variable_name.setValue(variable_name.getStringValue() + line.getStringValue().charAt(current_position));
                current_position++;
            }
        }
        if (!have_good_name) {
            print_error_message("Bad variable name", state);
        } else if (swallow_equals) {
            // Skip whitespace
            while (current_position < length
                    && Character.isWhitespace(line.getStringValue().charAt(current_position))) {
                current_position++;
            }
            if (line.getStringValue().charAt(current_position) == '=') {
                current_position++;
            } else {
                print_error_message("Missing equal sign", state);
                have_good_name = false;
            }
        }

        if (parameters.debug) {
            if (have_good_name) {
                System.out.println("# Got variable name: " + variable_name);
            } else {
                System.out.println("# Bad variable name: " + variable_name);
            }
        }

        shorten_line(line, current_position);
        return have_good_name;
    }

    /*********************************************************************
     * Function: get_file_name
     * Purpose:
     *********************************************************************/
    public static boolean get_file_name(AMutableString line, AMutableString variable_name, State state,
            Parameters parameters) {
        int current_position;
        int length;
        boolean have_good_name;

        current_position = 0;
        length = line.getStringValue().length();
        variable_name.setValue("");
        have_good_name = false;

        // Skip whitespace
        while (current_position < length && Character.isWhitespace(line.getStringValue().charAt(current_position))) {
            current_position++;
        }
        // Find file name
        while (current_position < length && (!Character.isWhitespace(line.getStringValue().charAt(current_position)))) {
            have_good_name = true;
            variable_name.setValue(variable_name.getStringValue() + line.getStringValue().charAt(current_position));
            current_position++;
        }
        if (!have_good_name) {
            print_error_message("Bad file name", state);
        }

        if (parameters.debug) {
            if (have_good_name) {
                System.out.println("# Got file name: " + variable_name.getStringValue());
            } else {
                System.out.println("# Bad file name: " + variable_name.getStringValue());
            }
        }

        shorten_line(line, current_position);
        return have_good_name;
    }

    /*********************************************************************
     * Function: get_expr
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static ExprTree get_expr(AMutableString line, State state, Parameters parameters,
            ClassAdObjectPool objectPool) throws IOException {
        int offset;
        ExprTree tree;
        ClassAdParser parser = new ClassAdParser(objectPool);
        StringLexerSource lexer_source = new StringLexerSource(line.getStringValue());

        tree = parser.parseExpression(lexer_source, false);
        offset = lexer_source.getCurrentLocation();
        shorten_line(line, offset);

        if (tree == null) {
            print_error_message("Missing expression", state);
        }

        return tree;
    }

    /*********************************************************************
     * Function: get_two_exprs
     * Purpose:
     *
     * @throws IOException
     *********************************************************************/
    public static void get_two_exprs(AMutableString line, ExprTreeHolder tree1, ExprTreeHolder tree2, State state,
            Parameters parameters, ClassAdObjectPool objectPool) throws IOException {
        int offset;
        ClassAdParser parser = new ClassAdParser(objectPool);
        StringLexerSource lexer_source = new StringLexerSource(line.getStringValue());

        tree1.setInnerTree(parser.parseExpression(lexer_source, false));
        if (tree1.getInnerTree() == null) {
            print_error_message("Couldn't parse first expression.", state);
            tree2.setInnerTree(null);
            throw new IOException();
        } else {
            if (parameters.debug) {
                System.out.print("# Tree1: ");
                print_expr(tree1, state, parameters, objectPool);
            }

            if (parser.peekToken() != TokenType.LEX_COMMA) {
                print_error_message("Missing comma.\n", state);
                tree1.setInnerTree(null);
                tree2.setInnerTree(null);
            } else {
                parser.consumeToken();
                tree2.setInnerTree(parser.parseNextExpression());
                offset = lexer_source.getCurrentLocation();
                shorten_line(line, offset);
                if (tree2.getInnerTree() == null) {
                    print_error_message("Couldn't parse second expression.", state);
                    tree1.setInnerTree(null);
                    throw new IOException();
                } else if (parameters.debug) {
                    System.out.print("# Tree2: ");
                    print_expr(tree2, state, parameters, objectPool);
                    System.out.print("# Tree1: ");
                    print_expr(tree1, state, parameters, objectPool);
                    System.out.println();
                }
            }
        }

        return;
    }

    /*********************************************************************
     * Function: print_expr
     * Purpose:
     *
     * @throws HyracksDataException
     *********************************************************************/
    public static void print_expr(ExprTree tree, State state, Parameters parameters, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        AMutableCharArrayString output = new AMutableCharArrayString();

        if (state.format == PrintFormat.print_Compact) {
            ClassAdUnParser unparser = new ClassAdUnParser(objectPool);
            unparser.unparse(output, tree);
        } else if (state.format == PrintFormat.print_Pretty) {
            PrettyPrint unparser = new PrettyPrint(objectPool);
            unparser.unparse(output, tree);
        } else if (state.format == PrintFormat.print_XML) {/*
                                                            * ClassAdXMLUnParser unparser = new
                                                            * ClassAdXMLUnParser();
                                                            * unparser.SetCompactSpacing(true);
                                                            * unparser.Unparse(output, tree);
                                                            * } else if (state.format ==
                                                            * PrintFormat.print_XMLPretty) {
                                                            * ClassAdXMLUnParser unparser = new
                                                            * ClassAdXMLUnParser();
                                                            * unparser.SetCompactSpacing(false);
                                                            * unparser.Unparse(output, tree);
                                                            */
        }
        System.out.println(output);
    }

    /*********************************************************************
     * Function: evaluate_expr
     * Purpose:
     *
     * @throws HyracksDataException
     *********************************************************************/
    public static boolean evaluate_expr(ExprTree tree, Value value, Parameters parameters, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        ClassAd classad = new ClassAd(objectPool);
        boolean success = false;
        classad.insert("internal___", tree);
        success = classad.evaluateAttr("internal___", value);
        classad.remove("internal___");
        return success;
    }

    /*********************************************************************
     * Function: shorten_line
     * Purpose:
     *********************************************************************/
    public static void shorten_line(AMutableString line, int offset) {
        // We have to be careful with substr() because with gcc 2.96, it likes to
        // assert/except if you give it values that are too large.
        if (offset < line.getStringValue().length()) {
            line.setValue(line.getStringValue().substring(offset));
        } else {
            line.setValue("");
        }
    }

    /*********************************************************************
     * Function: print_version
     * Purpose:
     *********************************************************************/
    public static void print_version() {
        AMutableString classad_version = new AMutableString(null);
        ClassAd.classAdLibraryVersion(classad_version);
        System.out.println("ClassAd Functional Tester v" + classad_version.getStringValue());
        return;
    }

    /*********************************************************************
     * Function: print_error_message
     * Purpose:
     *********************************************************************/

    /*********************************************************************
     * Function: print_error_message
     * Purpose:
     *********************************************************************/
    public static void print_error_message(String error, State state) {
        System.out.println("* Line " + state.lineNumber + ": " + error);
        state.number_of_errors++;
    }

    /*********************************************************************
     * Function: print_final_state
     * Purpose:
     *********************************************************************/
    public static void print_final_state(State state) {
        if (state.number_of_errors == 0) {
            System.out.println("No errors.");
        } else if (state.number_of_errors == 1) {
            System.out.println("1 error.");
        } else {
            System.out.println(state.number_of_errors + " errors");
        }
        return;
    }

    public static boolean line_is_comment(String line) {
        boolean is_comment;

        if (line.length() > 1 && line.charAt(0) == '/' && line.charAt(1) == '/') {
            is_comment = true;
        } else {
            is_comment = false;
        }
        return is_comment;
    }

    public static boolean expr_okay_for_xml_file(ExprTree tree, State state, Parameters parameters) {
        boolean is_okay;

        if (tree.getKind() == NodeKind.CLASSAD_NODE) {
            is_okay = true;
        } else if (tree.getKind() != NodeKind.EXPR_LIST_NODE) {
            is_okay = false;
            System.out.println("We have " + tree.getKind().ordinal());
        } else {
            ExprList list = (ExprList) tree;
            is_okay = true;
            for (ExprTree element : list.getExprList()) {

                if (element.getKind() != NodeKind.CLASSAD_NODE) {
                    System.out.println("Inside list, we have " + tree.getKind().ordinal());
                    is_okay = false;
                    break;
                }
            }
        }
        if (!is_okay) {
            print_error_message("writexml requires a ClassAd or list of ClassAds as an argument.", state);
        }
        return is_okay;
    }

}
