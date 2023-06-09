/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FunctionAndTypeResolver;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static com.facebook.presto.spi.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.LateralJoin.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.lateralJoin;
import static com.facebook.presto.sql.relational.Expressions.buildSwitch;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static java.util.Objects.requireNonNull;

/**
 * Scalar filter scan query is something like:
 * <pre>
 *     SELECT a,b,c FROM rel WHERE a = correlated1 AND b = correlated2
 * </pre>
 * <p>
 * This optimizer can rewrite to mark distinct and filter over a left outer join:
 * <p>
 * From:
 * <pre>
 * - LateralJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (scalar subquery) Project F
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Filter(CASE isDistinct WHEN true THEN true ELSE fail('Scalar sub-query has returned multiple rows'))
 *   - MarkDistinct(isDistinct)
 *     - LateralJoin (with correlation list: [C])
 *       - AssignUniqueId(adds symbol U)
 *         - (input) plan which produces symbols: [A, B, C]
 *       - non scalar subquery
 * </pre>
 * <p>
 * This must be run after {@link TransformCorrelatedScalarAggregationToJoin}
 */
public class TransformCorrelatedScalarSubquery
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(nonEmpty(correlation()));

    private final FunctionAndTypeResolver functionAndTypeResolver;

    public TransformCorrelatedScalarSubquery(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionAndTypeResolver = functionAndTypeManager.getFunctionAndTypeResolver();
    }

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        PlanNode subquery = context.getLookup().resolve(lateralJoinNode.getSubquery());

        if (!searchFrom(subquery, context.getLookup())
                .where(EnforceSingleRowNode.class::isInstance)
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .matches()) {
            return Result.empty();
        }

        PlanNode rewrittenSubquery = searchFrom(subquery, context.getLookup())
                .where(EnforceSingleRowNode.class::isInstance)
                .recurseOnlyWhen(ProjectNode.class::isInstance)
                .removeFirst();

        if (isAtMostScalar(rewrittenSubquery, context.getLookup())) {
            return Result.ofPlanNode(new LateralJoinNode(
                    lateralJoinNode.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    lateralJoinNode.getInput(),
                    rewrittenSubquery,
                    lateralJoinNode.getCorrelation(),
                    lateralJoinNode.getType(),
                    lateralJoinNode.getOriginSubqueryError()));
        }

        VariableReferenceExpression unique = context.getVariableAllocator().newVariable("unique", BIGINT);

        LateralJoinNode rewrittenLateralJoinNode = new LateralJoinNode(
                lateralJoinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                new AssignUniqueId(
                        lateralJoinNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        lateralJoinNode.getInput(),
                        unique),
                rewrittenSubquery,
                lateralJoinNode.getCorrelation(),
                lateralJoinNode.getType(),
                lateralJoinNode.getOriginSubqueryError());

        VariableReferenceExpression isDistinct = context.getVariableAllocator().newVariable("is_distinct", BooleanType.BOOLEAN);
        MarkDistinctNode markDistinctNode = new MarkDistinctNode(
                rewrittenLateralJoinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rewrittenLateralJoinNode,
                isDistinct,
                rewrittenLateralJoinNode.getInput().getOutputVariables(),
                Optional.empty());

        CallExpression fail = call(
                "fail",
                functionAndTypeResolver.lookupFunction("fail", fromTypes(INTEGER, VARCHAR)),
                UNKNOWN,
                new ConstantExpression((long) SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode(), INTEGER),
                new ConstantExpression(Slices.utf8Slice("Scalar sub-query has returned multiple rows"), VARCHAR));

        FilterNode filterNode = new FilterNode(
                markDistinctNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                markDistinctNode,
                buildSwitch(
                        isDistinct,
                        ImmutableList.of(specialForm(WHEN, BOOLEAN, TRUE_CONSTANT, TRUE_CONSTANT)),
                        Optional.of(call(CAST.name(), functionAndTypeResolver.lookupCast("CAST", UNKNOWN, BOOLEAN), BOOLEAN, fail)),
                        BOOLEAN));
//                castToRowExpression(new SimpleCaseExpression(
//                        createSymbolReference(isDistinct),
//                        ImmutableList.of(
//                                new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
//                        Optional.of(new Cast(
//                                new FunctionCall(
//                                        QualifiedName.of("fail"),
//                                        ImmutableList.of(
//                                                new LongLiteral(Integer.toString(SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode())),
//                                                new StringLiteral("Scalar sub-query has returned multiple rows"))),
//                                BOOLEAN)))));

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                filterNode,
                identityAssignments(lateralJoinNode.getOutputVariables())));
    }
}
