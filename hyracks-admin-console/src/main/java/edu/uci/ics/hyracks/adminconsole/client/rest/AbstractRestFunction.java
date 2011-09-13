package edu.uci.ics.hyracks.adminconsole.client.rest;

import com.google.gwt.http.client.Request;
import com.google.gwt.http.client.RequestBuilder;
import com.google.gwt.http.client.RequestCallback;
import com.google.gwt.http.client.RequestException;
import com.google.gwt.http.client.Response;
import com.google.gwt.http.client.URL;

import edu.uci.ics.hyracks.adminconsole.client.HyracksAdminConsole;

public abstract class AbstractRestFunction<T> {
    protected abstract void appendURLPath(StringBuilder buffer);

    protected native T packageResults(String result)
    /*-{
        return eval("[" + result + "]")[0].result;
    }-*/;

    public interface ResultCallback<T> {
        public void onSuccess(T result);

        public void onError(Throwable exception);
    }

    public void call(final ResultCallback<T> callback) throws RequestException {
        StringBuilder buffer = new StringBuilder();
        buffer.append(HyracksAdminConsole.INSTANCE.getServerConnection().getServerURLPrefix());
        buffer.append("/rest");
        appendURLPath(buffer);
        RequestBuilder builder = new RequestBuilder(RequestBuilder.GET, URL.encode(buffer.toString()));

        builder.sendRequest(null, new RequestCallback() {
            public void onError(Request request, Throwable exception) {
                callback.onError(exception);
            }

            public void onResponseReceived(Request request, Response response) {
                if (200 == response.getStatusCode()) {
                    callback.onSuccess(packageResults(response.getText()));
                } else {
                    callback.onError(null);
                }
            }
        });
    }
}