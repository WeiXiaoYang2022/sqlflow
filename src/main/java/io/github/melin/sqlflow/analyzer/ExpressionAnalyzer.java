package io.github.melin.sqlflow.analyzer;

import io.github.melin.sqlflow.AstVisitor;
import io.github.melin.sqlflow.SqlFlowException;
import io.github.melin.sqlflow.function.OperatorType;
import io.github.melin.sqlflow.metadata.MetadataService;
import io.github.melin.sqlflow.metadata.QualifiedObjectName;
import io.github.melin.sqlflow.parser.SqlFlowParser;
import io.github.melin.sqlflow.tree.*;
import io.github.melin.sqlflow.tree.expression.*;
import io.github.melin.sqlflow.tree.literal.LongLiteral;
import io.github.melin.sqlflow.tree.window.FrameBound;
import io.github.melin.sqlflow.tree.window.MeasureDefinition;
import io.github.melin.sqlflow.tree.window.VariableDefinition;
import io.github.melin.sqlflow.tree.window.WindowFrame;
import io.github.melin.sqlflow.type.RowType;
import io.github.melin.sqlflow.type.Type;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;

import static io.github.melin.sqlflow.analyzer.SemanticExceptions.missingAttributeException;
import static io.github.melin.sqlflow.analyzer.SemanticExceptions.semanticException;
import static io.github.melin.sqlflow.tree.window.FrameBound.Type.*;
import static io.github.melin.sqlflow.tree.window.WindowFrame.Type.*;
import static io.github.melin.sqlflow.type.UnknownType.UNKNOWN;
import static io.github.melin.sqlflow.util.NodeUtils.getSortItemsFromOrderBy;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/25 10:54 AM
 */
public class ExpressionAnalyzer {

    private final Analysis analysis;

    private final MetadataService metadataService;

    private final SqlFlowParser sqlFlowParser;

    private final Map<NodeRef<Expression>, Type> expressionTypes = new LinkedHashMap<>();

    private final Set<NodeRef<SubqueryExpression>> subqueries = new LinkedHashSet<>();
    private final Set<NodeRef<ExistsPredicate>> existsSubqueries = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, Type> expressionCoercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();

    private final Set<NodeRef<InPredicate>> subqueryInPredicates = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, ResolvedField> columnReferences = new LinkedHashMap<>();
    private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons = new LinkedHashSet<>();
    private final Set<NodeRef<FunctionCall>> windowFunctions = new LinkedHashSet<>();

    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();
    private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();
    private final Set<NodeRef<FunctionCall>> patternRecognitionFunctions = new LinkedHashSet<>();

    // Track referenced fields from source relation node
    private final Multimap<NodeRef<Node>, Field> referencedFields = HashMultimap.create();

    // Record fields prefixed with labels in row pattern recognition context
    private final Map<NodeRef<DereferenceExpression>, LabelPrefixedReference> labelDereferences = new LinkedHashMap<>();

    private final Map<NodeRef<Parameter>, Expression> parameters;
    private final Function<Node, Analysis.ResolvedWindow> getResolvedWindow;
    private final List<Field> sourceFields = new ArrayList<>();

    public Map<NodeRef<Expression>, Type> getExpressionCoercions() {
        return unmodifiableMap(expressionCoercions);
    }

    public Set<NodeRef<InPredicate>> getSubqueryInPredicates() {
        return unmodifiableSet(subqueryInPredicates);
    }

    public Set<NodeRef<SubqueryExpression>> getSubqueries() {
        return unmodifiableSet(subqueries);
    }

    public Set<NodeRef<ExistsPredicate>> getExistsSubqueries() {
        return unmodifiableSet(existsSubqueries);
    }

    public Map<NodeRef<Expression>, ResolvedField> getColumnReferences() {
        return unmodifiableMap(columnReferences);
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions() {
        return unmodifiableSet(typeOnlyCoercions);
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons() {
        return unmodifiableSet(quantifiedComparisons);
    }

    public Set<NodeRef<FunctionCall>> getWindowFunctions() {
        return unmodifiableSet(windowFunctions);
    }

    private ExpressionAnalyzer(Analysis analysis, MetadataService metadataService, SqlFlowParser sqlFlowParser) {
        this(analysis, metadataService, sqlFlowParser,
                analysis.getParameters(),
                analysis::getWindow);
    }

    ExpressionAnalyzer(
            Analysis analysis,
            MetadataService metadataService,
            SqlFlowParser sqlFlowParser,
            Map<NodeRef<Parameter>, Expression> parameters,
            Function<Node, Analysis.ResolvedWindow> getResolvedWindow) {
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.metadataService = requireNonNull(metadataService, "analysis is null");
        this.sqlFlowParser = requireNonNull(sqlFlowParser, "sqlParser is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.getResolvedWindow = requireNonNull(getResolvedWindow, "getResolvedWindow is null");
    }

    public Map<NodeRef<Expression>, Type> getExpressionTypes() {
        return unmodifiableMap(expressionTypes);
    }

    public Type setExpressionType(Expression expression, Type type) {
        requireNonNull(expression, "expression cannot be null");

        expressionTypes.put(NodeRef.of(expression), UNKNOWN);
        return UNKNOWN;
    }

    private Type getExpressionType(Expression expression) {
        requireNonNull(expression, "expression cannot be null");

        Type type = expressionTypes.get(NodeRef.of(expression));
        checkState(type != null, "Expression not yet analyzed: %s", expression);
        return type;
    }

    public Type analyze(Expression expression, Scope scope) {
        Visitor visitor = new Visitor(scope);
        return visitor.process(expression, Context.notInLambda(scope, CorrelationSupport.ALLOWED));
    }

    public Type analyze(Expression expression, Scope scope, CorrelationSupport correlationSupport) {
        Visitor visitor = new Visitor(scope);
        return visitor.process(expression, Context.notInLambda(scope, correlationSupport));
    }

    private Type analyze(Expression expression, Scope scope, Set<String> labels) {
        Visitor visitor = new Visitor(scope);
        return visitor.process(expression, Context.patternRecognition(scope, labels));
    }

    private Type analyze(Expression expression, Scope baseScope, Context context) {
        Visitor visitor = new Visitor(baseScope);
        return visitor.process(expression, context);
    }

    private void analyzeWindow(Analysis.ResolvedWindow window, Scope scope, Node originalNode, CorrelationSupport correlationSupport) {
        Visitor visitor = new Visitor(scope);
        //visitor.analyzeWindow(window, new StackableAstVisitor.StackableAstVisitorContext<>(Context.notInLambda(scope, correlationSupport)), originalNode);
    }

    public List<Field> getSourceFields() {
        return sourceFields;
    }

    private class Visitor
            extends AstVisitor<Type, Context> {
        // Used to resolve FieldReferences (e.g. during local execution planning)
        private final Scope baseScope;

        public Visitor(Scope baseScope) {
            this.baseScope = requireNonNull(baseScope, "baseScope is null");
        }

        @Override
        public Type process(Node node, @Nullable Context context) {
            if (node instanceof Expression) {
                // don't double process a node
                Type type = expressionTypes.get(NodeRef.of(((Expression) node)));
                if (type != null) {
                    return type;
                }
            }
            return super.process(node, context);
        }

        @Override
        public Type visitIdentifier(Identifier node, Context context) {
            try {
                ResolvedField resolvedField = context.getScope().resolveField(node, QualifiedName.of(node.getValue()));
                return handleResolvedField(node, resolvedField, context);
            } catch (SqlFlowException e) {
                return null;
            }
        }

        @Override
        public Type visitFunctionCall(FunctionCall node, Context context) {
            if (context.isPatternRecognition() && isPatternRecognitionFunction(node)) {
                return analyzePatternRecognitionFunction(node, context);
            }

            if (node.getProcessingMode().isPresent()) {
                ProcessingMode processingMode = node.getProcessingMode().get();
                if (!context.isPatternRecognition()) {
                    throw semanticException(processingMode, "%s semantics is not supported out of pattern recognition context", processingMode.getMode());
                }
            }

            /*if (node.getWindow().isPresent()) {
                Analysis.ResolvedWindow window = getResolvedWindow.apply(node);
                checkState(window != null, "no resolved window for: " + node);

                analyzeWindow(window, context, (Node) node.getWindow().get());
                windowFunctions.add(NodeRef.of(node));
            } else {
                if (node.isDistinct() && !plannerContext.getMetadata().isAggregationFunction(node.getName())) {
                    throw semanticException(FUNCTION_NOT_AGGREGATE, node, "DISTINCT is not supported for non-aggregation functions");
                }
            }*/

            if (node.getFilter().isPresent()) {
                Expression expression = node.getFilter().get();
                process(expression, context);
            }

            getCallArgumentTypes(node.getArguments(), context);

            /*if (QualifiedName.of("LISTAGG").equals(node.getName())) {
                // Due to fact that the LISTAGG function is transformed out of pragmatic reasons
                // in a synthetic function call, the type expression of this function call is evaluated
                // explicitly here in order to make sure that it is a varchar.
                List<Expression> arguments = node.getArguments();
                Expression expression = arguments.get(0);
                Type expressionType = process(expression, context);
                if (!(expressionType instanceof VarcharType)) {
                    throw semanticException(TYPE_MISMATCH, node, format("Expected expression of varchar, but '%s' has %s type", expression, expressionType.getDisplayName()));
                }
            }

            // must run after arguments are processed and labels are recorded
            if (context.getContext().isPatternRecognition() && plannerContext.getMetadata().isAggregationFunction(node.getName())) {
                validateAggregationLabelConsistency(node);
            }*/

            /*ResolvedFunction function;
            try {
                function = plannerContext.getMetadata().resolveFunction(session, node.getName(), argumentTypes);
            } catch (TrinoException e) {
                if (e.getLocation().isPresent()) {
                    // If analysis of any of the argument types (which is done lazily to deal with lambda
                    // expressions) fails, we want to report the original reason for the failure
                    throw e;
                }

                // otherwise, it must have failed due to a missing function or other reason, so we report an error at the
                // current location

                throw new TrinoException(e::getErrorCode, extractLocation(node), e.getMessage(), e);
            }*/

            /*if (function.getSignature().getName().equalsIgnoreCase(ARRAY_CONSTRUCTOR)) {
                // After optimization, array constructor is rewritten to a function call.
                // For historic reasons array constructor is allowed to have 254 arguments
                if (node.getArguments().size() > 254) {
                    throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for array constructor", function.getSignature().getName());
                }
            } else if (node.getArguments().size() > 127) {
                throw semanticException(TOO_MANY_ARGUMENTS, node, "Too many arguments for function call %s()", function.getSignature().getName());
            }*/

            if (node.getOrderBy().isPresent()) {
                for (SortItem sortItem : node.getOrderBy().get().getSortItems()) {
                    Type sortKeyType = process(sortItem.getSortKey(), context);
                    if (!sortKeyType.isOrderable()) {
                        throw semanticException(node, "ORDER BY can only be applied to orderable types (actual: %s)", sortKeyType.getDisplayName());
                    }
                }
            }

            /*BoundSignature signature = function.getSignature();
            for (int i = 0; i < argumentTypes.size(); i++) {
                Expression expression = node.getArguments().get(i);
                Type expectedType = signature.getArgumentTypes().get(i);
                requireNonNull(expectedType, format("Type '%s' not found", signature.getArgumentTypes().get(i)));
                if (node.isDistinct() && !expectedType.isComparable()) {
                    throw semanticException(TYPE_MISMATCH, node, "DISTINCT can only be applied to comparable types (actual: %s)", expectedType);
                }
                if (argumentTypes.get(i).hasDependency()) {
                    FunctionType expectedFunctionType = (FunctionType) expectedType;
                    process(expression, new StackableAstVisitorContext<>(context.getContext().expectingLambda(expectedFunctionType.getArgumentTypes())));
                } else {
                    Type actualType = plannerContext.getTypeManager().getType(argumentTypes.get(i).getTypeSignature());
                    coerceType(expression, actualType, expectedType, format("Function %s argument %d", function, i));
                }
            }
            accessControl.checkCanExecuteFunction(SecurityContext.of(session), node.getName().toString());

            resolvedFunctions.put(NodeRef.of(node), function);

            FunctionMetadata functionMetadata = plannerContext.getMetadata().getFunctionMetadata(function);
            if (functionMetadata.isDeprecated()) {
                warningCollector.add(new TrinoWarning(DEPRECATED_FUNCTION,
                        format("Use of deprecated function: %s: %s",
                                functionMetadata.getSignature().getName(),
                                functionMetadata.getDescription())));
            }

            Type type = signature.getReturnType();*/
            return setExpressionType(node, UNKNOWN);
        }

        private Type analyzePatternRecognitionFunction(FunctionCall node, Context context) {
            if (node.getWindow().isPresent()) {
                throw semanticException(node, "Cannot use OVER with %s pattern recognition function", node.getName());
            }
            if (node.getFilter().isPresent()) {
                throw semanticException(node, "Cannot use FILTER with %s pattern recognition function", node.getName());
            }
            if (node.getOrderBy().isPresent()) {
                throw semanticException(node, "Cannot use ORDER BY with %s pattern recognition function", node.getName());
            }
            if (node.isDistinct()) {
                throw semanticException(node, "Cannot use DISTINCT with %s pattern recognition function", node.getName());
            }
            String name = node.getName().getSuffix();
            if (node.getProcessingMode().isPresent()) {
                ProcessingMode processingMode = node.getProcessingMode().get();
                if (!name.equalsIgnoreCase("FIRST") && !name.equalsIgnoreCase("LAST")) {
                    throw semanticException(processingMode, "%s semantics is not supported with %s pattern recognition function", processingMode.getMode(), node.getName());
                }
            }

            patternRecognitionFunctions.add(NodeRef.of(node));

            switch (name.toUpperCase(ENGLISH)) {
                case "FIRST":
                case "LAST":
                case "PREV":
                case "NEXT":
                    if (node.getArguments().size() != 1 && node.getArguments().size() != 2) {
                        throw semanticException(node, "%s pattern recognition function requires 1 or 2 arguments", node.getName());
                    }
                    Type resultType = process(node.getArguments().get(0), context);
                    if (node.getArguments().size() == 2) {
                        process(node.getArguments().get(1), context);
                        // TODO the offset argument must be effectively constant, not necessarily a number. This could be extended with the use of ConstantAnalyzer.
                        if (!(node.getArguments().get(1) instanceof LongLiteral)) {
                            throw semanticException(node, "%s pattern recognition navigation function requires a number as the second argument", node.getName());
                        }
                        long offset = ((LongLiteral) node.getArguments().get(1)).getValue();
                        if (offset < 0) {
                            throw semanticException(node, "%s pattern recognition navigation function requires a non-negative number as the second argument (actual: %s)", node.getName(), offset);
                        }
                        if (offset > Integer.MAX_VALUE) {
                            throw semanticException(node, "The second argument of %s pattern recognition navigation function must not exceed %s (actual: %s)", node.getName(), Integer.MAX_VALUE, offset);
                        }
                    }
                    return setExpressionType(node, resultType);
                case "MATCH_NUMBER":
                    if (!node.getArguments().isEmpty()) {
                        throw semanticException(node, "MATCH_NUMBER pattern recognition function takes no arguments");
                    }
                    return setExpressionType(node, UNKNOWN);
                case "CLASSIFIER":
                    if (node.getArguments().size() > 1) {
                        throw semanticException(node, "CLASSIFIER pattern recognition function takes no arguments or 1 argument");
                    }
                    if (node.getArguments().size() == 1) {
                        Node argument = node.getArguments().get(0);
                        if (!(argument instanceof Identifier)) {
                            throw semanticException(argument, "CLASSIFIER function argument should be primary pattern variable or subset name. Actual: %s", argument.getClass().getSimpleName());
                        }
                        Identifier identifier = (Identifier) argument;
                        String label = label(identifier);
                        if (!context.getLabels().contains(label)) {
                            throw semanticException(argument, "%s is not a primary pattern variable or subset name", identifier.getValue());
                        }
                    }
                    return setExpressionType(node, UNKNOWN);
                default:
                    throw new IllegalStateException("unexpected pattern recognition function " + node.getName());
            }
        }

        @Override
        public Type visitWindowOperation(WindowOperation node, Context context) {
            Analysis.ResolvedWindow window = getResolvedWindow.apply(node);
            checkState(window != null, "no resolved window for: " + node);

            if (!window.getFrame().isPresent()) {
                throw semanticException(node, "Measure %s is not defined in the corresponding window", node.getName().getValue());
            }
            CanonicalizationAware<Identifier> canonicalName = CanonicalizationAware.canonicalizationAwareKey(node.getName());
            List<MeasureDefinition> matchingMeasures = window.getFrame().get().getMeasures().stream()
                    .filter(measureDefinition -> CanonicalizationAware.canonicalizationAwareKey(measureDefinition.getName()).equals(canonicalName))
                    .collect(toImmutableList());
            if (matchingMeasures.isEmpty()) {
                throw semanticException(node, "Measure %s is not defined in the corresponding window", node.getName().getValue());
            }
            if (matchingMeasures.size() > 1) {
                throw semanticException(node, "Measure %s is defined more than once", node.getName().getValue());
            }
            MeasureDefinition matchingMeasure = getOnlyElement(matchingMeasures);

            analyzeWindow(window, context, (Node) node.getWindow());
            return setExpressionType(node, UNKNOWN);
        }

        private void analyzeWindow(Analysis.ResolvedWindow window, Context context, Node originalNode) {
            // check no nested window functions
            ImmutableList.Builder<Node> childNodes = ImmutableList.builder();
            if (!window.isPartitionByInherited()) {
                childNodes.addAll(window.getPartitionBy());
            }
            if (!window.isOrderByInherited()) {
                window.getOrderBy().ifPresent(orderBy -> childNodes.addAll(orderBy.getSortItems()));
            }
            if (!window.isFrameInherited()) {
                window.getFrame().ifPresent(childNodes::add);
            }
            List<Expression> nestedWindowExpressions = ExpressionTreeUtils.extractWindowExpressions(childNodes.build());
            if (!nestedWindowExpressions.isEmpty()) {
                throw semanticException(nestedWindowExpressions.get(0), "Cannot nest window functions or row pattern measures inside window specification");
            }

            if (!window.isPartitionByInherited()) {
                for (Expression expression : window.getPartitionBy()) {
                    process(expression, context);
                    Type type = getExpressionType(expression);
                    if (!type.isComparable()) {
                        throw semanticException(expression, "%s is not comparable, and therefore cannot be used in window function PARTITION BY", type);
                    }
                }
            }

            if (!window.isOrderByInherited()) {
                for (SortItem sortItem : getSortItemsFromOrderBy(window.getOrderBy())) {
                    process(sortItem.getSortKey(), context);
                    Type type = getExpressionType(sortItem.getSortKey());
                    if (!type.isOrderable()) {
                        throw semanticException(sortItem, "%s is not orderable, and therefore cannot be used in window function ORDER BY", type);
                    }
                }
            }

            if (window.getFrame().isPresent() && !window.isFrameInherited()) {
                WindowFrame frame = window.getFrame().get();

                if (frame.getPattern().isPresent()) {
                    if (frame.getVariableDefinitions().isEmpty()) {
                        throw semanticException(frame, "Pattern recognition requires DEFINE clause");
                    }
                    if (frame.getType() != ROWS) {
                        throw semanticException(frame, "Pattern recognition requires ROWS frame type");
                    }
                    if (frame.getStart().getType() != CURRENT_ROW || !frame.getEnd().isPresent()) {
                        throw semanticException(frame, "Pattern recognition requires frame specified as BETWEEN CURRENT ROW AND ...");
                    }
                    PatternRecognitionAnalyzer.PatternRecognitionAnalysis analysis = PatternRecognitionAnalyzer.analyze(
                            frame.getSubsets(),
                            frame.getVariableDefinitions(),
                            frame.getMeasures(),
                            frame.getPattern().get(),
                            frame.getAfterMatchSkipTo());

                    PatternRecognitionAnalyzer.validateNoPatternAnchors(frame.getPattern().get());

                    // analyze expressions in MEASURES and DEFINE (with set of all labels passed as context)
                    for (VariableDefinition variableDefinition : frame.getVariableDefinitions()) {
                        Expression expression = variableDefinition.getExpression();
                        process(expression, context.patternRecognition(analysis.getAllLabels()));
                    }
                    for (MeasureDefinition measureDefinition : frame.getMeasures()) {
                        Expression expression = measureDefinition.getExpression();
                        process(expression, context.patternRecognition(analysis.getAllLabels()));
                    }

                    // validate pattern recognition expressions: MATCH_NUMBER() is not allowed in window
                    // this must run after the expressions in MEASURES and DEFINE are analyzed, and the patternRecognitionFunctions are recorded
                    PatternRecognitionAnalyzer.validateNoMatchNumber(frame.getMeasures(), frame.getVariableDefinitions(), patternRecognitionFunctions);

                    // TODO prohibited nesting: pattern recognition in frame end expression(?)
                } else {
                    if (!frame.getMeasures().isEmpty()) {
                        throw semanticException(frame, "Row pattern measures require PATTERN clause");
                    }
                    if (frame.getAfterMatchSkipTo().isPresent()) {
                        throw semanticException(frame.getAfterMatchSkipTo().get(), "AFTER MATCH SKIP clause requires PATTERN clause");
                    }
                    if (frame.getPatternSearchMode().isPresent()) {
                        throw semanticException(frame.getPatternSearchMode().get(), "%s modifier requires PATTERN clause", frame.getPatternSearchMode().get().getMode().name());
                    }
                    if (!frame.getSubsets().isEmpty()) {
                        throw semanticException(frame.getSubsets().get(0), "Union variable definitions require PATTERN clause");
                    }
                    if (!frame.getVariableDefinitions().isEmpty()) {
                        throw semanticException(frame.getVariableDefinitions().get(0), "Primary pattern variable definitions require PATTERN clause");
                    }
                }

                // validate frame start and end types
                FrameBound.Type startType = frame.getStart().getType();
                FrameBound.Type endType = frame.getEnd().orElse(new FrameBound(CURRENT_ROW)).getType();
                if (startType == UNBOUNDED_FOLLOWING) {
                    throw semanticException(frame, "Window frame start cannot be UNBOUNDED FOLLOWING");
                }
                if (endType == UNBOUNDED_PRECEDING) {
                    throw semanticException(frame, "Window frame end cannot be UNBOUNDED PRECEDING");
                }
                if ((startType == CURRENT_ROW) && (endType == PRECEDING)) {
                    throw semanticException(frame, "Window frame starting from CURRENT ROW cannot end with PRECEDING");
                }
                if ((startType == FOLLOWING) && (endType == PRECEDING)) {
                    throw semanticException(frame, "Window frame starting from FOLLOWING cannot end with PRECEDING");
                }
                if ((startType == FOLLOWING) && (endType == CURRENT_ROW)) {
                    throw semanticException(frame, "Window frame starting from FOLLOWING cannot end with CURRENT ROW");
                }

                // analyze frame offset values
                if (frame.getType() == ROWS) {
                    if (frame.getStart().getValue().isPresent()) {
                        Expression startValue = frame.getStart().getValue().get();
                        process(startValue, context);
                    }
                    if (frame.getEnd().isPresent() && frame.getEnd().get().getValue().isPresent()) {
                        Expression endValue = frame.getEnd().get().getValue().get();
                        process(endValue, context);
                    }
                } else if (frame.getType() == RANGE) {
                    if (frame.getStart().getValue().isPresent()) {
                        Expression startValue = frame.getStart().getValue().get();
                        analyzeFrameRangeOffset(startValue, frame.getStart().getType(), context, window, originalNode);
                    }
                    if (frame.getEnd().isPresent() && frame.getEnd().get().getValue().isPresent()) {
                        Expression endValue = frame.getEnd().get().getValue().get();
                        analyzeFrameRangeOffset(endValue, frame.getEnd().get().getType(), context, window, originalNode);
                    }
                } else if (frame.getType() == GROUPS) {
                    if (frame.getStart().getValue().isPresent()) {
                        if (!window.getOrderBy().isPresent()) {
                            throw semanticException(originalNode, "Window frame of type GROUPS PRECEDING or FOLLOWING requires ORDER BY");
                        }
                        Expression startValue = frame.getStart().getValue().get();
                        process(startValue, context);
                    }
                    if (frame.getEnd().isPresent() && frame.getEnd().get().getValue().isPresent()) {
                        if (!window.getOrderBy().isPresent()) {
                            throw semanticException(originalNode, "Window frame of type GROUPS PRECEDING or FOLLOWING requires ORDER BY");
                        }
                        Expression endValue = frame.getEnd().get().getValue().get();
                        process(endValue, context);
                    }
                } else {
                    throw semanticException(frame, "Unsupported frame type: " + frame.getType());
                }
            }
        }

        private void analyzeFrameRangeOffset(Expression offsetValue, FrameBound.Type boundType, Context context, Analysis.ResolvedWindow window, Node originalNode) {
            if (!window.getOrderBy().isPresent()) {
                throw semanticException(originalNode, "Window frame of type RANGE PRECEDING or FOLLOWING requires ORDER BY");
            }
            OrderBy orderBy = window.getOrderBy().get();
            if (orderBy.getSortItems().size() != 1) {
                throw semanticException(orderBy, "Window frame of type RANGE PRECEDING or FOLLOWING requires single sort item in ORDER BY (actual: %s)", orderBy.getSortItems().size());
            }

            process(offsetValue, context);
        }

        public void getCallArgumentTypes(List<Expression> arguments, Context context) {
            for (Expression argument : arguments) {
                if (argument instanceof LambdaExpression || argument instanceof BindExpression) {
                    ExpressionAnalyzer innerExpressionAnalyzer = new ExpressionAnalyzer(
                            analysis, metadataService, sqlFlowParser,
                            parameters,
                            getResolvedWindow);
                    if (context.isInLambda()) {
                        for (LambdaArgumentDeclaration lambdaArgument : context.getFieldToLambdaArgumentDeclaration().values()) {
                            innerExpressionAnalyzer.setExpressionType(lambdaArgument, getExpressionType(lambdaArgument));
                        }
                    }
                    innerExpressionAnalyzer.analyze(argument, baseScope, context);
                } else {
                    if (DereferenceExpression.isQualifiedAllFieldsReference(argument)) {
                        // to resolve `count(label.*)` correctly, we should skip the argument, like for `count(*)`
                        // process the argument but do not include it in the list
                        DereferenceExpression allRowsDereference = (DereferenceExpression) argument;
                        String label = label((Identifier) allRowsDereference.getBase());
                        if (!context.getLabels().contains(label)) {
                            throw semanticException(allRowsDereference.getBase(), "%s is not a primary pattern variable or subset name", label);
                        }
                        labelDereferences.put(NodeRef.of(allRowsDereference), new LabelPrefixedReference(label));
                    } else {
                        process(argument, context);
                    }
                }
            }
        }

        private String label(Identifier identifier) {
            return identifier.getCanonicalValue();
        }

        private Type handleResolvedField(Expression node, ResolvedField resolvedField, Context context) {
            if (!resolvedField.isLocal() && context.getCorrelationSupport() != CorrelationSupport.ALLOWED) {
                throw semanticException(node, "Reference to column '%s' from outer scope not allowed in this context", node);
            }

            FieldId fieldId = FieldId.from(resolvedField);
            Field field = resolvedField.getField();
            if (context.isInLambda()) {
                LambdaArgumentDeclaration lambdaArgumentDeclaration = context.getFieldToLambdaArgumentDeclaration().get(fieldId);
                if (lambdaArgumentDeclaration != null) {
                    // Lambda argument reference is not a column reference
                    lambdaArgumentReferences.put(NodeRef.of((Identifier) node), lambdaArgumentDeclaration);
                    return setExpressionType(node, field.getType());
                }
            }

            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                tableColumnReferences.put(field.getOriginTable().get(), field.getOriginColumnName().get());
            }

            sourceFields.add(field);

            fieldId.getRelationId()
                    .getSourceNode()
                    .ifPresent(source -> referencedFields.put(NodeRef.of(source), field));

            ResolvedField previous = columnReferences.put(NodeRef.of(node), resolvedField);
            checkState(previous == null, "%s already known to refer to %s", node, previous);

            return setExpressionType(node, field.getType());
        }

        @Override
        public Type visitSubqueryExpression(SubqueryExpression node, Context context) {
            Type type = analyzeSubquery(node, context);

            // the implied type of a scalar subquery is that of the unique field in the single-column row
            if (type instanceof RowType && ((RowType) type).getFields().size() == 1) {
                type = type.getTypeParameters().get(0);
            }

            setExpressionType(node, type);
            subqueries.add(NodeRef.of(node));
            return type;
        }

        @Override
        public Type visitDereferenceExpression(DereferenceExpression node, Context context) {
            if (DereferenceExpression.isQualifiedAllFieldsReference(node)) {
                throw semanticException(node, "<identifier>.* not allowed in this context");
            }

            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

            // If this Dereference looks like column reference, try match it to column first.
            if (qualifiedName != null) {
                // In the context of row pattern matching, fields are optionally prefixed with labels. Labels are irrelevant during type analysis.
                if (context.isPatternRecognition()) {
                    String label = label(qualifiedName.getOriginalParts().get(0));
                    if (context.getLabels().contains(label)) {
                        // In the context of row pattern matching, the name of row pattern input table cannot be used to qualify column names.
                        // (it can only be accessed in PARTITION BY and ORDER BY clauses of MATCH_RECOGNIZE). Consequentially, if a dereference
                        // expression starts with a label, the next part must be a column.
                        // Only strict column references can be prefixed by label. Labeled references to row fields are not supported.
                        QualifiedName unlabeledName = QualifiedName.of(qualifiedName.getOriginalParts().subList(1, qualifiedName.getOriginalParts().size()));
                        if (qualifiedName.getOriginalParts().size() > 2) {
                            throw semanticException(node, "Column %s prefixed with label %s cannot be resolved", unlabeledName, label);
                        }
                        Identifier unlabeled = qualifiedName.getOriginalParts().get(1);
                        Optional<ResolvedField> resolvedField = context.getScope().tryResolveField(node, unlabeledName);
                        if (!resolvedField.isPresent()) {
                            throw semanticException(node, "Column %s prefixed with label %s cannot be resolved", unlabeledName, label);
                        }
                        // Correlation is not allowed in pattern recognition context. Visitor's context for pattern recognition has CorrelationSupport.DISALLOWED,
                        // and so the following call should fail if the field is from outer scope.
                        Type type = process(unlabeled, context.notExpectingLabels());
                        labelDereferences.put(NodeRef.of(node), new LabelPrefixedReference(label, unlabeled));
                        return setExpressionType(node, type);
                    }
                    // In the context of row pattern matching, qualified column references are not allowed.
                    throw missingAttributeException(node, qualifiedName);
                }

                Scope scope = context.getScope();
                Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
                if (resolvedField.isPresent()) {
                    return handleResolvedField(node, resolvedField.get(), context);
                }
                if (!scope.isColumnReference(qualifiedName)) {
                    return null; // fix 字段来自常量字段
                    //throw missingAttributeException(node, qualifiedName);
                }
            }

            Type baseType = process(node.getBase(), context);
            if (!(baseType instanceof RowType)) {
                throw semanticException(node.getBase(), "Expression %s is not of type ROW", node.getBase());
            }

            RowType rowType = (RowType) baseType;
            Identifier field = node.getField().get();
            String fieldName = field.getValue();

            boolean foundFieldName = false;
            Type rowFieldType = null;
            for (RowType.Field rowField : rowType.getFields()) {
                if (fieldName.equalsIgnoreCase(rowField.getName().orElse(null))) {
                    if (foundFieldName) {
                        throw semanticException(field, "Ambiguous row field reference: " + fieldName);
                    }
                    foundFieldName = true;
                    rowFieldType = rowField.getType();
                }
            }

            if (rowFieldType == null) {
                throw missingAttributeException(node, qualifiedName);
            }

            return setExpressionType(node, rowFieldType);
        }

        @Override
        public Type visitFieldReference(FieldReference node, Context context) {
            ResolvedField field = baseScope.getField(node.getFieldIndex());
            return handleResolvedField(node, field, context);
        }

        @Override
        public Type visitComparisonExpression(ComparisonExpression node, Context context) {
            OperatorType operatorType;
            switch (node.getOperator()) {
                case EQUAL:
                case NOT_EQUAL:
                    operatorType = OperatorType.EQUAL;
                    break;
                case LESS_THAN:
                case GREATER_THAN:
                    operatorType = OperatorType.LESS_THAN;
                    break;
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN_OR_EQUAL:
                    operatorType = OperatorType.LESS_THAN_OR_EQUAL;
                    break;
                case IS_DISTINCT_FROM:
                    operatorType = OperatorType.IS_DISTINCT_FROM;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported comparison operator: " + node.getOperator());
            }
            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
        }

        @Override
        public Type visitIsNullPredicate(IsNullPredicate node, Context context) {
            process(node.getValue(), context);

            return setExpressionType(node, UNKNOWN);
        }

        @Override
        public Type visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
            process(node.getValue(), context);

            return setExpressionType(node, UNKNOWN);
        }

        @Override
        public Type visitNullIfExpression(NullIfExpression node, Context context) {
            Type firstType = process(node.getFirst(), context);
            process(node.getSecond(), context);

            return setExpressionType(node, firstType);
        }

        @Override
        public Type visitIfExpression(IfExpression node, Context context) {
            Type type;
            if (node.getFalseValue().isPresent()) {
                type = coerceToSingleType(context, node, "Result types for IF must be the same", node.getTrueValue(), node.getFalseValue().get());
            } else {
                type = process(node.getTrueValue(), context);
            }

            return setExpressionType(node, type);
        }

        @Override
        public Type visitSearchedCaseExpression(SearchedCaseExpression node, Context context) {
            Type type = UNKNOWN;
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        @Override
        public Type visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
            coerceCaseOperandToToSingleType(node, context);

            Type type = UNKNOWN;
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                Type whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        private void coerceCaseOperandToToSingleType(SimpleCaseExpression node, Context context) {
            List<WhenClause> whenClauses = node.getWhenClauses();
            for (WhenClause whenClause : whenClauses) {
                Expression whenOperand = whenClause.getOperand();
                process(whenOperand, context);
            }
        }

        @Override
        public Type visitCoalesceExpression(CoalesceExpression node, Context context) {
            Type type = UNKNOWN;
            return setExpressionType(node, type);
        }

        @Override
        public Type visitArithmeticUnary(ArithmeticUnaryExpression node, Context context) {
            switch (node.getSign()) {
                case PLUS:
                    process(node.getValue(), context);
                    return setExpressionType(node, UNKNOWN);
                case MINUS:
                    return getOperator(context, node, OperatorType.NEGATION, node.getValue());
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        public Type visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
            return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
        }

        @Override
        public Type visitCast(Cast node, Context context) {
            process(node.getExpression(), context);

            return setExpressionType(node, UNKNOWN);
        }

        @Override
        public Type visitInListExpression(InListExpression node, Context context) {
            setExpressionType(node, UNKNOWN);
            return UNKNOWN; // TODO: this really should a be relation type
        }

        @Override
        public Type visitLikePredicate(LikePredicate node, Context context) {
            process(node.getValue(), context);

            process(node.getPattern(), context);
            if (node.getEscape().isPresent()) {
                Expression escape = node.getEscape().get();
                process(escape, context);
            }

            return setExpressionType(node, UNKNOWN);
        }

        @Override
        public Type visitSubscriptExpression(SubscriptExpression node, Context context) {
            Type baseType = process(node.getBase(), context);
            // Subscript on Row hasn't got a dedicated operator. Its Type is resolved by hand.
            if (baseType instanceof RowType) {
                if (!(node.getIndex() instanceof LongLiteral)) {
                    throw semanticException(node.getIndex(), "Subscript expression on ROW requires a constant index");
                }
                process(node.getIndex(), context);
                int indexValue = toIntExact(((LongLiteral) node.getIndex()).getValue());
                if (indexValue <= 0) {
                    throw semanticException(node.getIndex(), "Invalid subscript index: %s. ROW indices start at 1", indexValue);
                }
                List<Type> rowTypes = baseType.getTypeParameters();
                if (indexValue > rowTypes.size()) {
                    throw semanticException(node.getIndex(), "Subscript index out of bounds: %s, max value is %s", indexValue, rowTypes.size());
                }
                return setExpressionType(node, rowTypes.get(indexValue - 1));
            }

            // Subscript on Array or Map uses an operator to resolve Type.
            return getOperator(context, node, OperatorType.SUBSCRIPT, node.getBase(), node.getIndex());
        }

        private Type getOperator(Context context, Expression node, OperatorType operatorType, Expression... arguments) {
            for (Expression expression : arguments) {
                process(expression, context);
            }

            return setExpressionType(node, UNKNOWN);
        }

        private Type coerceToSingleType(Context context, Node node, String message, Expression first, Expression second) {
            if (first != null) {
                process(first, context);
            }
            if (second != null) {
                process(second, context);
            }

            return UNKNOWN;
        }
    }

    public static boolean isPatternRecognitionFunction(FunctionCall node) {
        QualifiedName qualifiedName = node.getName();
        if (qualifiedName.getParts().size() > 1) {
            return false;
        }
        Identifier identifier = qualifiedName.getOriginalParts().get(0);
        if (identifier.isDelimited()) {
            return false;
        }
        String name = identifier.getValue().toUpperCase(ENGLISH);
        return name.equals("FIRST") ||
                name.equals("LAST") ||
                name.equals("PREV") ||
                name.equals("NEXT") ||
                name.equals("CLASSIFIER") ||
                name.equals("MATCH_NUMBER");
    }

    private Type analyzeSubquery(SubqueryExpression node, Context context) {
        if (context.isInLambda()) {
            throw semanticException(node, "Lambda expression cannot contain subqueries");
        }
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadataService, sqlFlowParser);
        Scope subqueryScope = Scope.builder()
                .withParent(context.getScope())
                .build();
        Scope queryScope = analyzer.analyze(node.getQuery(), subqueryScope);

        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        for (int i = 0; i < queryScope.getRelationType().getAllFieldCount(); i++) {
            Field field = queryScope.getRelationType().getFieldByIndex(i);
                if (field.getName().isPresent()) {
                    fields.add(RowType.field(field.getName().get(), field.getType()));
                } else {
                    fields.add(RowType.field(field.getType()));
                }
        }

        sourceFields.addAll(queryScope.getRelationType().getVisibleFields());
        return RowType.from(fields.build());
    }

    public static ExpressionAnalysis analyzeExpression(
            Scope scope,
            Analysis analysis,
            MetadataService metadataService,
            SqlFlowParser sqlFlowParser,
            Expression expression) {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(analysis, metadataService, sqlFlowParser);
        analyzer.analyze(expression, scope);

        updateAnalysis(analysis, analyzer);
        analysis.addExpressionFields(expression, analyzer.getSourceFields());
        analyzer.getSourceFields().forEach(field -> {
            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                Analysis.SourceColumn sourceColumn = new Analysis.SourceColumn(field.getOriginTable().get(),
                        field.getOriginColumnName().get());
                analysis.addOriginField(sourceColumn, field.getLocation());
            }
        });

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeExpressions(MetadataService metadataService, SqlFlowParser sqlFlowParser,
                                                        Iterable<Expression> expressions,
                                                        Map<NodeRef<Parameter>, Expression> parameters) {
        Analysis analysis = new Analysis(null, parameters);
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(analysis, metadataService, sqlFlowParser);
        for (Expression expression : expressions) {
            analyzer.analyze(
                    expression,
                    Scope.builder()
                            .withRelationType(RelationId.anonymous(), new RelationType())
                            .build());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeWindow(
            Scope scope,
            Analysis analysis,
            MetadataService metadataService,
            SqlFlowParser sqlFlowParser,
            CorrelationSupport correlationSupport,
            Analysis.ResolvedWindow window,
            Node originalNode) {
        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(analysis, metadataService, sqlFlowParser);
        analyzer.analyzeWindow(window, scope, originalNode, correlationSupport);

        updateAnalysis(analysis, analyzer);

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getWindowFunctions());
    }

    private static void updateAnalysis(Analysis analysis, ExpressionAnalyzer analyzer) {
        analysis.addColumnReferences(analyzer.getColumnReferences());
    }

    private static class Context {
        private final Scope scope;

        // functionInputTypes and nameToLambdaDeclarationMap can be null or non-null independently. All 4 combinations are possible.

        // The list of types when expecting a lambda (i.e. processing lambda parameters of a function); null otherwise.
        // Empty list represents expecting a lambda with no arguments.
        private final List<Type> functionInputTypes;
        // The mapping from names to corresponding lambda argument declarations when inside a lambda; null otherwise.
        // Empty map means that the all lambda expressions surrounding the current node has no arguments.
        private final Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration;

        // Primary row pattern variables and named unions (subsets) of variables
        // necessary for the analysis of expressions in the context of row pattern recognition
        private final Set<String> labels;

        private final CorrelationSupport correlationSupport;

        private Context(
                Scope scope,
                List<Type> functionInputTypes,
                Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration,
                Set<String> labels,
                CorrelationSupport correlationSupport) {
            this.scope = requireNonNull(scope, "scope is null");
            this.functionInputTypes = functionInputTypes;
            this.fieldToLambdaArgumentDeclaration = fieldToLambdaArgumentDeclaration;
            this.labels = labels;
            this.correlationSupport = requireNonNull(correlationSupport, "correlationSupport is null");
        }

        public static Context notInLambda(Scope scope, CorrelationSupport correlationSupport) {
            return new Context(scope, null, null, null, correlationSupport);
        }

        public Context inLambda(Scope scope, Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration) {
            return new Context(scope, null, requireNonNull(fieldToLambdaArgumentDeclaration, "fieldToLambdaArgumentDeclaration is null"), labels, correlationSupport);
        }

        public Context expectingLambda(List<Type> functionInputTypes) {
            return new Context(scope, requireNonNull(functionInputTypes, "functionInputTypes is null"), this.fieldToLambdaArgumentDeclaration, labels, correlationSupport);
        }

        public Context notExpectingLambda() {
            return new Context(scope, null, this.fieldToLambdaArgumentDeclaration, labels, correlationSupport);
        }

        public static Context patternRecognition(Scope scope, Set<String> labels) {
            return new Context(scope, null, null, requireNonNull(labels, "labels is null"), CorrelationSupport.DISALLOWED);
        }

        public Context patternRecognition(Set<String> labels) {
            return new Context(scope, functionInputTypes, fieldToLambdaArgumentDeclaration, requireNonNull(labels, "labels is null"), CorrelationSupport.DISALLOWED);
        }

        public Context notExpectingLabels() {
            return new Context(scope, functionInputTypes, fieldToLambdaArgumentDeclaration, null, correlationSupport);
        }

        Scope getScope() {
            return scope;
        }

        public boolean isInLambda() {
            return fieldToLambdaArgumentDeclaration != null;
        }

        public boolean isExpectingLambda() {
            return functionInputTypes != null;
        }

        public boolean isPatternRecognition() {
            return labels != null;
        }

        public Map<FieldId, LambdaArgumentDeclaration> getFieldToLambdaArgumentDeclaration() {
            checkState(isInLambda());
            return fieldToLambdaArgumentDeclaration;
        }

        public List<Type> getFunctionInputTypes() {
            checkState(isExpectingLambda());
            return functionInputTypes;
        }

        public Set<String> getLabels() {
            checkState(isPatternRecognition());
            return labels;
        }

        public CorrelationSupport getCorrelationSupport() {
            return correlationSupport;
        }
    }

    public static class LabelPrefixedReference {
        private final String label;
        private final Optional<Identifier> column;

        public LabelPrefixedReference(String label, Identifier column) {
            this(label, Optional.of(requireNonNull(column, "column is null")));
        }

        public LabelPrefixedReference(String label) {
            this(label, Optional.empty());
        }

        private LabelPrefixedReference(String label, Optional<Identifier> column) {
            this.label = requireNonNull(label, "label is null");
            this.column = requireNonNull(column, "column is null");
        }

        public String getLabel() {
            return label;
        }

        public Optional<Identifier> getColumn() {
            return column;
        }
    }
}
