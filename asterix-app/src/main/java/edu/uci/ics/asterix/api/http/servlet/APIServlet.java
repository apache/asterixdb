package edu.uci.ics.asterix.api.http.servlet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.asterix.api.common.SessionConfig;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.aql.translator.AqlTranslator;
import edu.uci.ics.asterix.aql.translator.QueryResult;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.result.ResultReader;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;

public class APIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";

    private static final String HTML_HEADER_TEMPLATE = "<!DOCTYPE html>"
            + "<html lang=\"en\">"
            + "<head>"
            + "<meta name=\"description\" content=\"ASTERIX WEB PAGE\" />"
            + "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">"
            + "<link href='http://fonts.googleapis.com/css?family=Bitter|PT+Sans+Caption|Open+Sans' rel='stylesheet' type='text/css'>"
            + "<script src=\"http://code.jquery.com/jquery.min.js\"></script>"
            + "<script src=\"http://www.jacklmoore.com/autosize/jquery.autosize.js\"></script>"
            + "  "
            + "<link href=\"http://twitter.github.com/bootstrap/assets/css/bootstrap.css\" rel=\"stylesheet\" type=\"text/css\" />"
            + "<link href=\"http://twitter.github.com/bootstrap/assets/css/bootstrap-responsive.css\" rel=\"stylesheet\">"
            + ""
            + "  "
            + "<script src=\"http://twitter.github.com/bootstrap/assets/js/bootstrap.js\"></script>"
            + ""
            + "<script type=\"text/javascript\">"
            + "$(document).ready(function(){"
            + "   $('textarea').autosize();"
            + "});"
            + "</script>"
            + ""
            + "<meta charset=utf-8 />"
            + "<title>ASTERIX Demo</title>"
            + "<style id=\"jsbin-css\">"
            + "body {"
            + "    background: none repeat scroll 0 0 white;"
            + "    color: #222222;"
            + "    font-family: 'Bitter';"
            + "    font-size: 14px;"
            + "    line-height: 17px;"
            + "    width: 100%;"
            + "}"
            + ""
            + ".content {"
            + "    margin-top: 70px;"
            + "}"
            + ""
            + "label.query, label.result {"
            + "    font-size: 24px;"
            + "    padding-bottom: 10px;"
            + "    font-weight: bold;"
            + "}"
            + ""
            + "div.host {"
            + "    float: left;"
            + "    margin: 0 100px 0 10px;"
            + "}"
            + ""
            + "div.port {"
            + "}"
            + ""
            + "div.left {"
            + "    float: left;"
            + "    width: 320px;"
            + "    padding: 0 20px 0 10px;"
            + "}"
            + ""
            + "div.right {"
            + "}"
            + ""
            + "button.btn {"
            + "    clear: both;"
            + "    float: left;"
            + "    margin: 20px 0 0 10px;;"
            + "}"
            + ""
            + "textarea.query {"
            + "    -webkit-box-sizing: border-box;"
            + "    -moz-box-sizing: border-box;"
            + "    -ms-box-sizing: border-box;"
            + "    box-sizing: border-box;"
            + "    font-size: 16px;"
            + "    line-height: 20px;"
            + "    font-family: bitter, helvetica;"
            + "    width: 100%;"
            + "    padding: 10px;"
            + "    color: #999;"
            + "    resize: none;"
            + "    border: 10px solid #eee;"
            + "}"
            + ""
            + "textarea.query:focus {"
            + "    outline: none;"
            + "    color: #333;"
            + "}"
            + ""
            + "label {"
            + "    padding-top: 10px;"
            + "}"
            + ""
            + "input[type=text] {"
            + "    height: 20px;"
            + "}"
            + ""
            + ""
            + "div.output label.heading {"
            + "    font-size: 24px;"
            + "    margin-top: 2px;"
            + "    padding-bottom: 10px;"
            + "    font-weight: bold;"
            + "}"
            + ""
            + "div.output .message {"
            + "    -webkit-box-sizing: border-box;"
            + "    -moz-box-sizing: border-box;"
            + "    -ms-box-sizing: border-box;"
            + "    box-sizing: border-box;"
            + "    -webkit-border-radius: 4px 4px 4px 4px;"
            + "    -moz-border-radius: 4px 4px 4px 4px;"
            + "    -ms-border-radius: 4px 4px 4px 4px;"
            + "    border-radius: 4px 4px 4px 4px;"
            + "    color: #000;"
            + "    resize: none;"
            + "    border: 1px solid #eee;"
            + "}"
            + ""
            + "div.error label.heading {"
            + "    color: #ff2020;"
            + "    font-size: 24px;"
            + "    margin-top: 2px;"
            + "    padding-bottom: 10px;"
            + "    font-weight: bold;"
            + "}"
            + ""
            + "div.error .message {"
            + "    -webkit-box-sizing: border-box;"
            + "    -moz-box-sizing: border-box;"
            + "    -ms-box-sizing: border-box;"
            + "    box-sizing: border-box;"
            + "    border-color: rgba(82, 168, 236, 0.8);"
            + "    outline: 0;"
            + "    outline: thin dotted 9;"
            + ""
            + "    -webkit-box-shadow: inset 0 1px 1px rgba(250, 0, 0, 0.075), 0 0 8px rgba(255, 0, 0, 0.8);"
            + "    -moz-box-shadow: inset 0 1px 1px rgba(250, 0, 0, 0.075), 0 0 8px rgba(255, 0, 0, 1.0);"
            + "    box-shadow: inset 0 1px 1px rgba(250, 0, 0, 0.075), 0 0 8px rgba(255, 0, 0, 1.0);"
            + "    color: #000;"
            + "    resize: none;"
            + "    border: 1px solid #eee;"
            + "    margin-top: 7px;"
            + "    padding: 20px 20px 20px 20px;"
            + "}"
            + ""
            + ".footer {"
            + "   margin-top: 40px;"
            + "}"
            + ""
            + ".footer .line {"
            + "    border-top: 1px solid #EEEEEE;"
            + "    bottom: 20px;"
            + "    height: 10px;"
            + "    left: 0;"
            + "    position: fixed;"
            + "    width: 100%;"
            + "}"
            + ""
            + ".footer .content {"
            + "    background: none repeat scroll 0 0 #FFFFFF;"
            + "    bottom: 0;"
            + "    color: #666666;"
            + "    font-size: 12px;"
            + "    height: 25px;"
            + "    left: 0;"
            + "    padding-top: 5px;"
            + "    position: fixed;"
            + "    width: 100%;"
            + "}"
            + ""
            + ".footer .content .left {"
            + "    padding-left: 20px;"
            + "    float: left;"
            + "}"
            + ""
            + ".footer .content .right {"
            + "    padding-right: 20px;"
            + "    float: right;"
            + "}</style></head>"
            + "<body>"
            + "  <div class=\"navbar navbar-inverse navbar-fixed-top\">"
            + "    <div class=\"navbar-inner\">"
            + "      <div class=\"container\">"
            + "        <a class=\"btn btn-navbar\" data-toggle=\"collapse\" data-target=\".nav-collapse\">"
            + "          <span class=\"icon-bar\"></span>"
            + "          <span class=\"icon-bar\"></span>"
            + "          <span class=\"icon-bar\"></span>"
            + "        </a>"
            + "        <a class=\"brand\" href=\"#\">ASTERIX</a>"
            + "        <div class=\"nav-collapse collapse\">"
            + "          <ul class=\"nav\">"
            + "            <li><a href=\"#\">Open source</a></li>"
            + "            <li><a href=\"#about\">File issues</a></li>"
            + "            <li><a href=\"#contact\">Contact</a></li>"
            + "          </ul>"
            + "        </div><!--/.nav-collapse -->"
            + "      </div>"
            + "    </div>"
            + "  </div>"
            + "";
    private static final String HTML_FORM_CONTENT_TEMPLATE = "  <div class=\"content\">"
            + "    <div class=\"container\">"
            + "      <div class=\"row-fluid\">"
            + "        <div class=\"span6\">"
            + "          <form class=\"form-horizontal\" method=\"post\">"
            + "            <div>"
            + "              <label class=\"query\">Query</label>"
            + "              <textarea rows=\"20\" name=\"query\" class=\"query\" value=\"%s\" placeholder=\"Type your AQL query ...\"></textarea>"
            + "            </div>"
            + "            <div>"
            + "              <div class=\"host\">"
            + "                <label>Host</label><input type=\"text\" name=\"hyracks-ip\" placeholder=\"IP Address or hostname\"/>"
            + "              </div>"
            + "              <div class=\"port\">"
            + "                <label>Port</label><input type=\"text\" name=\"hyracks-port\" placeholder=\"Port number\"/>"
            + "              </div>"
            + "            </div>"
            + "            <div>"
            + "              <div class=\"left\">"
            + "                <label class=\"checkbox\"><input type=\"checkbox\" name=\"print-expr-tree\" value=\"true\" /> Print parsed expressions</label>"
            + "              </div>"
            + "              <div class=\"right\">"
            + "                <label class=\"checkbox\"><input type=\"checkbox\" name=\"print-rewritten-expr-tree\" value=\"true\" /> Print rewritten expressions</label>"
            + "              </div>"
            + "            </div>"
            + "            <div>"
            + "              <div class=\"left\">"
            + "                <label class=\"checkbox\"><input type=\"checkbox\" name=\"print-logical-plan\" value=\"true\" /> Print logical plan</label>"
            + "              </div>"
            + "              <div class=\"right\">"
            + "                <label class=\"checkbox\"><input type=\"checkbox\" name=\"print-optimized-logical-plan\" value=\"true\" /> Print optimized logical plan</label>"
            + "              </div>"
            + "            </div>"
            + "            <div>"
            + "              <div class=\"left\">"
            + "                <label class=\"checkbox\"><input type=\"checkbox\" name=\"print-job\" value=\"true\" /> Print hyracks job</label>"
            + "              </div>"
            + "              <div class=\"right\">"
            + "                <label class=\"checkbox\"><input type=\"checkbox\" name=\"display-result\" value=\"true\" /> Display error/results</label>"
            + "              </div>"
            + "            </div>"
            + "            <button type=\"submit\" class=\"btn btn-danger\">Execute</button>"
            + "          </form>"
            + "        </div>";

    private static final String HTML_EMPTY_OUTPUT_TEMPLATE = "        <div class=\"span6\">"
            + "          <div class=\"output\">"
            + "            <label class=\"heading\">Output</label>"
            + "            <div class=\"message\">"
            + "            </div>"
            + "          </div>"
            + "        </div>";

    private static final String HTML_OUTPUT_TEMPLATE = "<div class=\"span6\">"
            + "  <div class=\"output\">"
            + "    <label class=\"heading\">Output</label>"
            + "    <table class=\"table table-bordered table-striped\">"
            + "      %s"
            + "    </table>"
            + "  </div>"
            + "</div>";

    private static final String HTML_FOOTER_TEMPLATE = "      </div>"
            + "    </div>"
            + "  </div>"
            + "  <div class=\"footer\">"
            + "    <section class=\"line\"><hr></section>"
            + "    <section class=\"content\">"
            + "      <section class=\"left\">"
            + "        Developed by ASTERIX group"
            + "      </section>"
            + "      <section class=\"right\">"
            + "        &copy; Copyright 2012 University of California, Irvine"
            + "      </section>"
            + "    </section>"
            + "  </div>"
            + "</body>"
            + "</html>";

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String query = request.getParameter("query");
        String printExprParam = request.getParameter("print-expr-tree");
        String printRewrittenExprParam = request.getParameter("print-rewritten-expr-tree");
        String printLogicalPlanParam = request.getParameter("print-logical-plan");
        String printOptimizedLogicalPlanParam = request.getParameter("print-optimized-logical-plan");
        String printJob = request.getParameter("print-job");
        String strIP = request.getParameter("hyracks-ip");
        String strPort = request.getParameter("hyracks-port");
        String strDisplayResult = request.getParameter("display-result");
        int port = Integer.parseInt(strPort);
        PrintWriter out = response.getWriter();
        response.setContentType("text/html");
        out.println("<H1>Input statements:</H1>");
        printInHtml(out, query);
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        ResultReader resultReader;
        try {
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
                if (hcc == null) {
                    hcc = new HyracksConnection(strIP, port);
                    context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
                }
                resultReader = new ResultReader(hcc, out);
                new Thread(resultReader).start();
            }
            AQLParser parser = new AQLParser(query);
            List<Statement> aqlStatements = parser.Statement();
            SessionConfig sessionConfig = new SessionConfig(port, true, isSet(printExprParam),
                    isSet(printRewrittenExprParam), isSet(printLogicalPlanParam),
                    isSet(printOptimizedLogicalPlanParam), false, true, isSet(printJob));
            MetadataManager.INSTANCE.init();
            AqlTranslator aqlTranslator = new AqlTranslator(aqlStatements, out, sessionConfig, DisplayFormat.HTML);
            List<QueryResult> executionResults = null;
            double duration = 0;
            long startTime = System.currentTimeMillis();
            executionResults = aqlTranslator.compileAndExecute(hcc, resultReader);
            long endTime = System.currentTimeMillis();
            duration = (endTime - startTime) / 1000.00;
            out.println("<PRE>Duration of all jobs: " + duration + "</PRE>");
        } catch (ParseException pe) {
            String message = pe.getMessage();
            message = message.replace("<", "&lt");
            message = message.replace(">", "&gt");
            out.println("SyntaxError:" + message);
            int pos = message.indexOf("line");
            if (pos > 0) {
                int columnPos = message.indexOf(",", pos + 1 + "line".length());
                int lineNo = Integer.parseInt(message.substring(pos + "line".length() + 1, columnPos));
                String line = query.split("\n")[lineNo - 1];
                out.println("==> " + line);
            }
        } catch (Exception e) {
            e.printStackTrace();
            out.println(e.getMessage());
        }
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        PrintWriter out = response.getWriter();
        response.setContentType("text/html");
        final String form = HTML_HEADER_TEMPLATE + String.format(HTML_FORM_CONTENT_TEMPLATE, "")
                + HTML_EMPTY_OUTPUT_TEMPLATE + HTML_FOOTER_TEMPLATE;
        out.println(form);
    }

    private static boolean isSet(String requestParameter) {
        return (requestParameter != null && requestParameter.equals("true"));
    }

    private static void printInHtml(PrintWriter out, String s) {
        out.println("<PRE>");
        out.println(s);
        out.println("</PRE>");
    }

    private void displayFile(File localFile, PrintWriter out) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(localFile));
        String inputLine = reader.readLine();
        int i = 0;
        while (inputLine != null) {
            out.println(inputLine);
            inputLine = reader.readLine();
            i++;
            if (i > 500) {
                out.println("...");
                out.println("SKIPPING THE REST OF THE RESULTS");
                break;
            }
        }
        reader.close();
    }
}
