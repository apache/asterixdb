package edu.uci.ics.asterix.api.http.servlet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;

public class AsterixSDKServlet extends HttpServlet {

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        
        String requestURI = request.getRequestURI();
        String contentType = "application/javascript";
        if (requestURI.endsWith("html")) {
            contentType = "text/html";
        }
        response.setContentType(contentType);
        response.setCharacterEncoding("utf-8");

        PrintWriter out = response.getWriter();

        if (requestURI.startsWith("/sdk/static/")) {
            InputStream is = APIServlet.class.getResourceAsStream(requestURI);
            if (is == null) {
                response.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }

            InputStreamReader isr = new InputStreamReader(is);
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append("\n");
                line = br.readLine();
            }

            out.println(sb.toString());

            return;
        }
    }

}
