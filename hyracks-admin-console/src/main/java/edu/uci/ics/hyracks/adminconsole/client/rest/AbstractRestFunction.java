/*
 * Copyright 2009-2010 by The Regents of the University of California
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