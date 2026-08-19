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
package org.apache.asterix.app.result.fields;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.api.http.server.AbstractQueryApiServlet.ResultStatus;
import org.apache.asterix.api.http.server.ResultUtil;
import org.apache.asterix.app.result.ExecutionError;
import org.apache.asterix.app.result.ExecutionWarning;
import org.apache.asterix.app.result.ResponseMetrics;
import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.api.ICodedMessage;
import org.apache.asterix.common.api.IResponseFieldPrinter;
import org.apache.asterix.translator.ExecutionPlans;
import org.apache.asterix.translator.IStatementExecutor.ResultDelivery;
import org.apache.asterix.translator.IStatementExecutor.ResultSetInfo;
import org.apache.asterix.translator.IStatementExecutor.StatementInfo;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.result.IResultSet;

/**
 * Prints what each statement produced, as a {@value #FIELD_NAME} array holding one object per statement: its position,
 * kind, signature, rows or handle, plans, outcome, error and metrics. A request carrying one statement is printed by the
 * pre-existing path instead; see {@code NCQueryServiceServlet#useMultiStatementResponse}.
 * <p>
 * {@code metrics} comes after {@code results} because the row count and size are only final once the rows are streamed.
 */
public class NcStatementsPrinter implements IResponseFieldPrinter {

    public static final String FIELD_NAME = "statements";
    public static final String POSITION_FIELD_NAME = "statement";
    public static final String KIND_FIELD_NAME = "kind";
    /**
     * What a statement reports for itself. The time the request took, and whether it failed, belong to the request: a
     * statement that failed says so with its status and its errors.
     */
    private static final Set<MetricsPrinter.Metrics> STATEMENT_METRICS =
            EnumSet.complementOf(EnumSet.of(MetricsPrinter.Metrics.ELAPSED_TIME, MetricsPrinter.Metrics.EXECUTION_TIME,
                    MetricsPrinter.Metrics.ERROR_COUNT));

    /** Indent of a statement object's fields, one level deeper than the array itself. */
    private static final String FIELD_INDENT = "\t\t\t";

    /** One field of a statement object. Collected so that the separators cannot be got wrong. */
    private interface FieldEmitter {
        void emit(PrintWriter pw) throws HyracksDataException;
    }

    private final IApplicationContext appCtx;
    private final List<StatementInfo> statements;
    private final IResultSet resultSet;
    private final ResultDelivery delivery;
    private final SessionOutput sessionOutput;
    private final Charset resultCharset;
    private final boolean printSignature;
    /** Included in each statement's handle; null where a handle is identified by job alone. */
    private final String requestId;

    public NcStatementsPrinter(IApplicationContext appCtx, List<StatementInfo> statements, IResultSet resultSet,
            ResultDelivery delivery, SessionOutput sessionOutput, Charset resultCharset, boolean printSignature,
            String requestId) {
        this.appCtx = appCtx;
        this.statements = statements;
        this.resultSet = resultSet;
        this.delivery = delivery;
        this.sessionOutput = sessionOutput;
        this.resultCharset = resultCharset;
        this.printSignature = printSignature;
        this.requestId = requestId;
    }

    @Override
    public void print(PrintWriter pw) throws HyracksDataException {
        pw.print("\t\"");
        pw.print(FIELD_NAME);
        pw.print("\": [\n");
        try {
            boolean first = true;
            for (StatementInfo statement : statements) {
                // the request's own dataverse declaration is not the client's, so it gets no entry
                if (statement.getPosition() == 0) {
                    continue;
                }
                printStatement(pw, statement, !first);
                first = false;
            }
        } finally {
            // closed even where a statement failed to print, so that what was already sent stays a valid response;
            // the failure is reported for the request, which is where a partly written response is completed
            pw.print("\n\t]");
        }
    }

    private void printStatement(PrintWriter pw, StatementInfo statement, boolean separatorBefore)
            throws HyracksDataException {
        Stats stats = statement.getStats() == null ? new Stats() : statement.getStats();
        boolean failed = statement.getError() != null;
        ResultSetInfo resultSetInfo = statement.getResultSet();
        ExecutionPlans plans = statement.getPlans();
        List<FieldEmitter> fields = new ArrayList<>();
        // opened before anything of this statement is written, so that a reader that cannot be opened - the job of a
        // cancelled request is gone - leaves neither a half written statement nor a separator with nothing after it
        ResultReader rows = resultSetInfo != null && delivery == ResultDelivery.IMMEDIATE
                ? new ResultReader(resultSet, resultSetInfo.getJobId(), resultSetInfo.getResultSetId()) : null;

        fields.add(w -> printField(w, POSITION_FIELD_NAME, String.valueOf(statement.getPosition())));
        fields.add(w -> printField(w, KIND_FIELD_NAME, quoted(statementKind(statement))));
        // a statement that returns rows describes them, as the flat response does; the signature is the default one
        // unless the client asked for a typed one, exactly as SignaturePrinter.newInstance decides for a request
        if (printSignature && resultSetInfo != null) {
            fields.add((plans == null ? SignaturePrinter.INSTANCE : SignaturePrinter.newInstance(plans))::print);
        }
        if (resultSetInfo != null) {
            if (delivery == ResultDelivery.IMMEDIATE) {
                fields.add(w -> printRows(w, rows, resultSetInfo, stats));
            } else if (delivery == ResultDelivery.DEFERRED) {
                // a handle per statement, each naming that statement's own job
                fields.add(new ResultHandlePrinter(sessionOutput,
                        new ResultHandle(resultSetInfo.getJobId(), resultSetInfo.getResultSetId(), requestId))::print);
            }
        }
        if (plans != null) {
            fields.add(new PlansPrinter(plans, sessionOutput.config().getPlanFormat())::print);
        }
        fields.add(w -> printField(w, StatusPrinter.FIELD_NAME,
                quoted((failed ? ResultStatus.FATAL : ResultStatus.SUCCESS).str())));
        if (failed) {
            fields.add(errorsPrinter(statement.getError())::print);
        }
        if (!statement.getWarnings().isEmpty()) {
            fields.add(warningsPrinter(statement.getWarnings())::print);
        }
        // built when the field is printed, not now: the row count and size are only final once the rows are streamed
        fields.add(w -> new MetricsPrinter(statementMetrics(stats, failed), resultCharset, STATEMENT_METRICS).print(w));
        if (stats.getJobProfile() != null) {
            fields.add(new ProfilePrinter(stats.getJobProfile())::print);
        }

        if (separatorBefore) {
            pw.print(",\n");
        }
        pw.print("\t\t{\n");
        try {
            for (int i = 0; i < fields.size(); i++) {
                fields.get(i).emit(pw);
                if (i + 1 != fields.size()) {
                    pw.print(",\n");
                }
            }
        } finally {
            pw.print("\n\t\t}");
        }
    }

    /** Streams this statement's rows; every statement has already run by the time this is reached. */
    private void printRows(PrintWriter pw, ResultReader reader, ResultSetInfo resultSetInfo, Stats stats)
            throws HyracksDataException {
        pw.print(FIELD_INDENT);
        pw.print(quoted(ResultsPrinter.FIELD_NAME));
        pw.print(": ");
        // no result decorators: this printer names the field itself, so the decorators that name and number it must not
        SessionOutput undecorated = new SessionOutput(sessionOutput.config(), pw);
        ResultUtil.printResults(appCtx, reader, undecorated, stats, resultSetInfo.getRecordType());
    }

    /** The errors of a failed statement. Overridable for a deployment that represents errors in its own form. */
    protected IResponseFieldPrinter errorsPrinter(Throwable error) {
        List<ICodedMessage> errors = Collections.singletonList(ExecutionError.of(error));
        return new ErrorsPrinter(errors);
    }

    /** The warnings of a statement. Overridable for the same reason as {@link #errorsPrinter(Throwable)}. */
    protected IResponseFieldPrinter warningsPrinter(List<Warning> statementWarnings) {
        List<ICodedMessage> warnings = new ArrayList<>(statementWarnings.size());
        statementWarnings.forEach(warning -> warnings.add(ExecutionWarning.of(warning)));
        return new WarningsPrinter(warnings);
    }

    /** The kind of a statement, or for an extension statement the name that identifies it. */
    private static String statementKind(StatementInfo statement) {
        if (statement.getName() != null) {
            return statement.getName();
        }
        return statement.getKind() == null ? "unknown" : statement.getKind().getDisplayName();
    }

    /**
     * The metrics of one statement. Elapsed and execution time are measured around the whole request, so they are
     * reported there. Public because a deployment recording a statement elsewhere records the same figures.
     */
    public static ResponseMetrics statementMetrics(Stats stats, boolean failed) {
        return ResponseMetrics.of(0, 0, stats.getCount(), stats.getSize(), stats.getProcessedObjects(), failed ? 1 : 0,
                stats.getTotalWarningsCount(), stats.getCompileTimeNanos(), stats.getQueueWaitTimeNanos(),
                stats.getBufferCacheHitRatio(), stats.getBufferCachePageReadCount(), stats.getCloudReadRequestsCount(),
                stats.getCloudPagesReadCount(), stats.getCloudPagesPersistedCount());
    }

    private static void printField(PrintWriter pw, String name, String rawValue) {
        pw.print(FIELD_INDENT);
        pw.print(quoted(name));
        pw.print(": ");
        pw.print(rawValue);
    }

    private static String quoted(String value) {
        return '"' + value + '"';
    }

    @Override
    public String getName() {
        return FIELD_NAME;
    }
}
