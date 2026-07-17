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

package org.apache.asterix.lang.common.statement;

import java.util.List;
import java.util.Locale;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.lang.common.base.AbstractStatement;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.expression.FieldBinding;
import org.apache.asterix.lang.common.expression.RecordConstructor;
import org.apache.asterix.lang.common.util.ExpressionUtils;
import org.apache.asterix.lang.common.util.LangRecordParseUtil;
import org.apache.asterix.lang.common.visitor.base.ILangVisitor;
import org.apache.asterix.metadata.entities.Index;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmDoubleNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.util.annotations.AiProvenance;

public class AnalyzeStatement extends AbstractStatement {

    private static final String SAMPLE_FIELD_NAME = "sample";
    private static final String SAMPLE_LOW = "low";
    private static final String SAMPLE_MEDIUM = "medium";
    private static final String SAMPLE_HIGH = "high";
    private static final int SAMPLE_LOW_SIZE = 1063;
    private static final int SAMPLE_MEDIUM_SIZE = SAMPLE_LOW_SIZE * 4;
    private static final int SAMPLE_HIGH_SIZE = SAMPLE_MEDIUM_SIZE * 4;
    private static final int SAMPLE_DEFAULT_SIZE = SAMPLE_LOW_SIZE;

    private static final String SAMPLE_SEED_FIELD_NAME = "sample-seed";
    // "sample-method" takes the same vocabulary persisted in the index metadata (Index.SampleIndexDetails.
    // SampleMethod): "full-scan" or "random".
    private static final String SAMPLE_METHOD_FIELD_NAME = "sample-method";

    private final Namespace namespace;
    private final String datasetName;
    private final AdmObjectNode options;

    public AnalyzeStatement(Namespace namespace, String datasetName, RecordConstructor options)
            throws CompilationException {
        this.namespace = namespace;
        this.datasetName = datasetName;
        this.options = options == null ? null : validateOptions(options);
    }

    private static AdmObjectNode validateOptions(RecordConstructor options) throws CompilationException {
        final List<FieldBinding> fbList = options.getFbList();
        for (int i = 0; i < fbList.size(); i++) {
            FieldBinding binding = fbList.get(i);
            String key = LangRecordParseUtil.exprToStringLiteral(binding.getLeftExpr()).getStringValue();
            Expression value = binding.getRightExpr();
            switch (key) {
                case SAMPLE_FIELD_NAME:
                    if (value.getKind() != Expression.Kind.LITERAL_EXPRESSION) {
                        throw new CompilationException(ErrorCode.INVALID_SAMPLE_SIZE);
                    }
                    break;
                case SAMPLE_SEED_FIELD_NAME:
                    if (value.getKind() != Expression.Kind.LITERAL_EXPRESSION
                            && value.getKind() != Expression.Kind.UNARY_EXPRESSION) {
                        throw new CompilationException(ErrorCode.INVALID_SAMPLE_SEED);
                    }
                    break;
                case SAMPLE_METHOD_FIELD_NAME:
                    if (value.getKind() != Expression.Kind.LITERAL_EXPRESSION) {
                        throw new CompilationException(ErrorCode.INVALID_SAMPLE_METHOD);
                    }
                    break;
                default:
                    throw new CompilationException(ErrorCode.INVALID_PARAM, key);
            }
        }
        return (ExpressionUtils.toNode(options));
    }

    @Override
    public Kind getKind() {
        return Kind.ANALYZE;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public DataverseName getDataverseName() {
        return namespace == null ? null : namespace.getDataverseName();
    }

    public String getDatasetName() {
        return datasetName;
    }

    public int getSampleSize() throws CompilationException {
        IAdmNode sampleSizeNode = getOption(SAMPLE_FIELD_NAME);
        if (sampleSizeNode == null) {
            return SAMPLE_DEFAULT_SIZE;
        }
        switch (sampleSizeNode.getType()) {
            case STRING:
                String s = ((AdmStringNode) sampleSizeNode).get();
                switch (s.toLowerCase(Locale.ROOT)) {
                    case SAMPLE_LOW:
                        return SAMPLE_LOW_SIZE;
                    case SAMPLE_MEDIUM:
                        return SAMPLE_MEDIUM_SIZE;
                    case SAMPLE_HIGH:
                        return SAMPLE_HIGH_SIZE;
                    default:
                        throw new CompilationException(ErrorCode.INVALID_SAMPLE_SIZE);
                }
            case BIGINT:
                int v = (int) ((AdmBigIntNode) sampleSizeNode).get();
                if (!isValidSampleSize(v)) {
                    throw new CompilationException(ErrorCode.OUT_OF_RANGE_SAMPLE_SIZE, SAMPLE_LOW_SIZE,
                            SAMPLE_HIGH_SIZE);
                }
                return v;
            case DOUBLE:
                v = (int) ((AdmDoubleNode) sampleSizeNode).get();
                if (!isValidSampleSize(v)) {
                    throw new CompilationException(ErrorCode.OUT_OF_RANGE_SAMPLE_SIZE, SAMPLE_LOW_SIZE,
                            SAMPLE_HIGH_SIZE);
                }
                return v;
            default:
                throw new CompilationException(ErrorCode.WITH_FIELD_MUST_BE_OF_TYPE, SAMPLE_FIELD_NAME,
                        BuiltinType.ASTRING.getTypeName(), sampleSizeNode.getType().toString());
        }
    }

    public long getOrCreateSampleSeed() throws AlgebricksException {
        IAdmNode sampleSeedNode = getOption(SAMPLE_SEED_FIELD_NAME);
        return sampleSeedNode != null ? getSampleSeed(sampleSeedNode) : createSampleSeed();
    }

    private long getSampleSeed(IAdmNode sampleSeedNode) throws CompilationException {
        switch (sampleSeedNode.getType()) {
            case BIGINT:
                return ((AdmBigIntNode) sampleSeedNode).get();
            case DOUBLE:
                return (long) ((AdmDoubleNode) sampleSeedNode).get();
            case STRING:
                String s = ((AdmStringNode) sampleSeedNode).get();
                try {
                    return Long.parseLong(s);
                } catch (NumberFormatException e) {
                    throw new CompilationException(ErrorCode.INVALID_SAMPLE_SEED);
                }
            default:
                throw new CompilationException(ErrorCode.WITH_FIELD_MUST_BE_OF_TYPE, SAMPLE_SEED_FIELD_NAME,
                        BuiltinType.AINT64.getTypeName(), sampleSeedNode.getType().toString());
        }
    }

    /**
     * The requested sampling method, using the same vocabulary persisted in the index metadata
     * ("full-scan" / "random"), from the {@code sample-method} option. Defaults to {@code RANDOM}.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_4_8, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "sample-method WITH option aligned with the stored SampleMethod enum")
    public Index.SampleIndexDetails.SampleMethod getSampleMethod() throws CompilationException {
        IAdmNode methodNode = getOption(SAMPLE_METHOD_FIELD_NAME);
        if (methodNode == null) {
            return Index.SampleIndexDetails.SampleMethod.RANDOM;
        }
        if (methodNode.getType() != ATypeTag.STRING) {
            throw new CompilationException(ErrorCode.WITH_FIELD_MUST_BE_OF_TYPE, SAMPLE_METHOD_FIELD_NAME,
                    BuiltinType.ASTRING.getTypeName(), methodNode.getType().toString());
        }
        String method = ((AdmStringNode) methodNode).get().toLowerCase(Locale.ROOT);
        try {
            return Index.SampleIndexDetails.SampleMethod.fromMetadataName(method);
        } catch (IllegalArgumentException e) {
            throw new CompilationException(ErrorCode.INVALID_SAMPLE_METHOD);
        }
    }

    private long createSampleSeed() {
        return System.nanoTime() + System.identityHashCode(this);
    }

    private boolean isValidSampleSize(int v) {
        return v >= SAMPLE_LOW_SIZE && v <= SAMPLE_HIGH_SIZE * 4;
    }

    private IAdmNode getOption(String optionName) {
        return options != null ? options.get(optionName) : null;
    }

    @Override
    public <R, T> R accept(ILangVisitor<R, T> visitor, T arg) throws CompilationException {
        return visitor.visit(this, arg);
    }

    @Override
    public byte getCategory() {
        return Category.DDL;
    }
}
