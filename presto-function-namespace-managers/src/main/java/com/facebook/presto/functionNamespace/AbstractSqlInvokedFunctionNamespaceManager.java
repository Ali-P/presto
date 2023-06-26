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
package com.facebook.presto.functionNamespace;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.functionNamespace.execution.SqlFunctionExecutors;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.analyzer.TypeSignatureProvider;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.AggregationFunctionMetadata;
import com.facebook.presto.spi.function.BoundVariables;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionImplementationType;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.RemoteScalarFunctionImplementation;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SignatureBinder;
import com.facebook.presto.spi.function.SpecializedFunctionKey;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionHandle;
import com.facebook.presto.spi.function.SqlInvokedAggregationFunctionImplementation;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.common.type.TypeUtils.resolveTypes;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.SignatureBinder.applyBoundVariables;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractSqlInvokedFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlInvokedFunction>
{
    private final ConcurrentMap<FunctionNamespaceTransactionHandle, FunctionCollection> transactions = new ConcurrentHashMap<>();

    private final String catalogName;
    private final SqlFunctionExecutors sqlFunctionExecutors;
    private final LoadingCache<QualifiedObjectName, Collection<SqlInvokedFunction>> functions;
    // TODO: Cache user defined types at query level as well.
    private final LoadingCache<QualifiedObjectName, UserDefinedType> userDefinedTypes;
    private final LoadingCache<SqlFunctionHandle, FunctionMetadata> metadataByHandle;
    private final LoadingCache<SqlFunctionHandle, ScalarFunctionImplementation> implementationByHandle;
    private final Map<Signature, SpecializedFunctionKey> specializedFunctionKeyCache = new ConcurrentHashMap<>();

    public AbstractSqlInvokedFunctionNamespaceManager(String catalogName, SqlFunctionExecutors sqlFunctionExecutors, SqlInvokedFunctionNamespaceManagerConfig config)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.sqlFunctionExecutors = requireNonNull(sqlFunctionExecutors, "sqlFunctionExecutors is null");
        requireNonNull(config, "config is null");
        this.functions = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<QualifiedObjectName, Collection<SqlInvokedFunction>>()
                {
                    @Override
                    @ParametersAreNonnullByDefault
                    public Collection<SqlInvokedFunction> load(QualifiedObjectName functionName)
                    {
                        Collection<SqlInvokedFunction> functions = fetchFunctionsDirect(functionName);
                        for (SqlInvokedFunction function : functions) {
                            metadataByHandle.put(function.getRequiredFunctionHandle(), sqlInvokedFunctionToMetadata(function));
                        }
                        return functions;
                    }
                });
        this.userDefinedTypes = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getTypeCacheExpiration().toMillis(), MILLISECONDS)
                .build(CacheLoader.from(this::fetchUserDefinedTypeDirect));

        this.metadataByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlFunctionHandle, FunctionMetadata>()
                {
                    @Override
                    @ParametersAreNonnullByDefault
                    public FunctionMetadata load(SqlFunctionHandle functionHandle)
                    {
                        return fetchFunctionMetadataDirect(functionHandle);
                    }
                });
        this.implementationByHandle = CacheBuilder.newBuilder()
                .expireAfterWrite(config.getFunctionInstanceCacheExpiration().toMillis(), MILLISECONDS)
                .build(new CacheLoader<SqlFunctionHandle, ScalarFunctionImplementation>()
                {
                    @Override
                    public ScalarFunctionImplementation load(SqlFunctionHandle functionHandle)
                    {
                        return fetchFunctionImplementationDirect(functionHandle);
                    }
                });
    }

    protected abstract Collection<SqlInvokedFunction> fetchFunctionsDirect(QualifiedObjectName functionName);

    protected abstract UserDefinedType fetchUserDefinedTypeDirect(QualifiedObjectName typeName);

    protected abstract FunctionMetadata fetchFunctionMetadataDirect(SqlFunctionHandle functionHandle);

    protected abstract ScalarFunctionImplementation fetchFunctionImplementationDirect(SqlFunctionHandle functionHandle);

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        sqlFunctionExecutors.setBlockEncodingSerde(blockEncodingSerde);
    }

    @Override
    public final FunctionNamespaceTransactionHandle beginTransaction()
    {
        UuidFunctionNamespaceTransactionHandle transactionHandle = UuidFunctionNamespaceTransactionHandle.create();
        transactions.put(transactionHandle, new FunctionCollection());
        return transactionHandle;
    }

    @Override
    public final void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional commit is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
        // Transactional rollback is not supported yet.
        transactions.remove(transactionHandle);
    }

    @Override
    public final Collection<SqlInvokedFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName)
    {
        checkCatalog(functionName);
        if (transactionHandle.isPresent()) {
            return transactions.get(transactionHandle.get()).loadAndGetFunctionsTransactional(functionName);
        }
        return fetchFunctionsDirect(functionName);
    }

    @Override
    public Optional<UserDefinedType> getUserDefinedType(QualifiedObjectName typeName)
    {
        try {
            return Optional.of(userDefinedTypes.getUnchecked(typeName));
        }
        catch (UncheckedExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PrestoException) {
                PrestoException prestoException = (PrestoException) cause;
                if (prestoException.getErrorCode().equals(NOT_FOUND.toErrorCode())) {
                    return Optional.empty();
                }
                throw prestoException;
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error getting UserDefinedType: %s", typeName), cause);
        }
    }

    @Override
    public final FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature, TypeManager typeManager)
    {
        checkCatalog(signature.getName());
        // This is the only assumption in this class that we're dealing with sql-invoked regular function.
        if (transactionHandle.isPresent()) {
            return transactions.get(transactionHandle.get()).getFunctionHandle(signature, typeManager);
        }
        FunctionCollection collection = new FunctionCollection();
        collection.loadAndGetFunctionsTransactional(signature.getName());
        return collection.getFunctionHandle(signature, typeManager);
    }

    @Override
    public final FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle, TypeManager typeManager)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        try {
            SqlFunctionHandle sqlFunctionHandle = (SqlFunctionHandle) functionHandle;
            FunctionMetadata metadata = metadataByHandle.getUnchecked(sqlFunctionHandle);
            return updateMetadataWithSignature(metadata, sqlFunctionHandle.getSignature());
        }
        catch (UncheckedExecutionException e) {
            throw convertToPrestoException(e, format("Error getting FunctionMetadata for handle: %s", functionHandle));
        }
    }

    @Override
    public final ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkCatalog(functionHandle);
        checkArgument(functionHandle instanceof SqlFunctionHandle, "Unsupported FunctionHandle type '%s'", functionHandle.getClass().getSimpleName());
        try {
            return implementationByHandle.getUnchecked((SqlFunctionHandle) functionHandle);
        }
        catch (UncheckedExecutionException e) {
            throw convertToPrestoException(e, format("Error getting ScalarFunctionImplementation for handle: %s", functionHandle));
        }
    }

    @Override
    public CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager)
    {
        checkArgument(functionHandle instanceof SqlFunctionHandle, format("Expect SqlFunctionHandle, got %s", functionHandle.getClass()));
        FunctionMetadata functionMetadata = getFunctionMetadata(functionHandle, typeManager);
        return sqlFunctionExecutors.executeFunction(
                source,
                getScalarFunctionImplementation(functionHandle),
                input,
                channels,
                functionMetadata.getArgumentTypes().stream().map(typeManager::getType).collect(toImmutableList()),
                typeManager.getType(functionMetadata.getReturnType()));
    }

    private static PrestoException convertToPrestoException(UncheckedExecutionException exception, String failureMessage)
    {
        Throwable cause = exception.getCause();
        if (cause instanceof PrestoException) {
            return (PrestoException) cause;
        }
        return new PrestoException(GENERIC_INTERNAL_ERROR, failureMessage, cause);
    }

    protected String getCatalogName()
    {
        return catalogName;
    }

    protected void checkCatalog(SqlFunction function)
    {
        checkCatalog(function.getSignature().getName());
    }

    protected void checkCatalog(QualifiedObjectName functionName)
    {
        checkCatalog(functionName.getCatalogSchemaName());
    }

    protected void checkCatalog(FunctionHandle functionHandle)
    {
        checkCatalog(functionHandle.getCatalogSchemaName());
    }

    protected void checkCatalog(CatalogSchemaName functionNamespace)
    {
        checkArgument(
                catalogName.equals(functionNamespace.getCatalogName()),
                "Catalog [%s] is not served by this FunctionNamespaceManager, expected: %s",
                functionNamespace.getCatalogName(),
                catalogName);
    }

    protected void refreshFunctionsCache(QualifiedObjectName functionName)
    {
        functions.refresh(functionName);
    }

    protected void checkFunctionLanguageSupported(SqlInvokedFunction function)
    {
        if (!sqlFunctionExecutors.getSupportedLanguages().contains(function.getRoutineCharacteristics().getLanguage())) {
            throw new PrestoException(GENERIC_USER_ERROR, format("Catalog %s does not support functions implemented in language %s", catalogName, function.getRoutineCharacteristics().getLanguage()));
        }
    }

    protected FunctionMetadata sqlInvokedFunctionToMetadata(SqlInvokedFunction function)
    {
        return new FunctionMetadata(
                function.getSignature().getName(),
                function.getSignature().getArgumentTypes(),
                function.getParameters().stream()
                        .map(Parameter::getName)
                        .collect(toImmutableList()),
                function.getSignature().getReturnType(),
                function.getSignature().getKind(),
                function.getRoutineCharacteristics().getLanguage(),
                getFunctionImplementationType(function),
                function.isDeterministic(),
                function.isCalledOnNullInput(),
                function.getVersion());
    }
    protected FunctionMetadata updateMetadataWithSignature(FunctionMetadata oldMetadata, Signature signature)
    {
        checkArgument(oldMetadata.getLanguage().isPresent(), "Metadata is not present");
        return new FunctionMetadata(
                oldMetadata.getName(),
                signature.getArgumentTypes(),
                oldMetadata.getArgumentNames().orElseGet(ArrayList::new),
                signature.getReturnType(),
                signature.getKind(),
                oldMetadata.getLanguage().get(),
                oldMetadata.getImplementationType(),
                oldMetadata.isDeterministic(),
                oldMetadata.isCalledOnNullInput(),
                oldMetadata.getVersion());
    }

    protected FunctionImplementationType getFunctionImplementationType(SqlInvokedFunction function)
    {
        return sqlFunctionExecutors.getFunctionImplementationType(function.getRoutineCharacteristics().getLanguage());
    }

    protected ScalarFunctionImplementation sqlInvokedFunctionToImplementation(SqlInvokedFunction function)
    {
        FunctionImplementationType implementationType = getFunctionImplementationType(function);
        switch (implementationType) {
            case SQL:
                return new SqlInvokedScalarFunctionImplementation(function.getBody());
            case THRIFT:
            case GRPC:
                checkArgument(function.getFunctionHandle().isPresent(), "Need functionHandle to get function implementation");
                return new RemoteScalarFunctionImplementation(function.getFunctionHandle().get(), function.getRoutineCharacteristics().getLanguage(), implementationType);
            case JAVA:
                throw new IllegalStateException(
                        format("SqlInvokedFunction %s has BUILTIN implementation type but %s cannot manage BUILTIN functions", function.getSignature().getName(), this.getClass()));
            case CPP:
                throw new IllegalStateException(format("Presto coordinator can not resolve implementation of CPP UDF functions"));
            default:
                throw new IllegalStateException(format("Unknown function implementation type: %s", implementationType));
        }
    }

    protected AggregationFunctionImplementation sqlInvokedFunctionToAggregationImplementation(
            SqlInvokedFunction function,
            TypeManager typeManager)
    {
        checkArgument(
                function.getSignature().getKind() == AGGREGATE,
                "Need an AGGREGATE function input to get aggregation function implementation");

        FunctionImplementationType implementationType = getFunctionImplementationType(function);
        switch (implementationType) {
            case SQL:
            case THRIFT:
            case GRPC:
            case JAVA:
                throw new IllegalStateException(format(
                        "Aggregate SqlInvokedFunction %s has %s implementation type, which is not supported by %s",
                        function.getSignature().getName(),
                        getClass().getSimpleName(),
                        implementationType));
            case CPP:
                checkArgument(
                        function.getAggregationMetadata().isPresent(),
                        "Need aggregationMetadata to get aggregation function implementation");

                AggregationFunctionMetadata aggregationMetadata = function.getAggregationMetadata().get();
                return new SqlInvokedAggregationFunctionImplementation(
                        typeManager.getType(aggregationMetadata.getIntermediateType()),
                        typeManager.getType(function.getSignature().getReturnType()),
                        aggregationMetadata.isOrderSensitive());
            default:
                throw new IllegalStateException(format("Unknown function implementation type: %s", implementationType));
        }
    }

    private Collection<SqlInvokedFunction> fetchFunctions(QualifiedObjectName functionName)
    {
        try {
            return functions.getUnchecked(functionName);
        }
        catch (UncheckedExecutionException e) {
            throw convertToPrestoException(e, format("Error fetching functions: %s", functionName));
        }
    }
    private SpecializedFunctionKey getSpecializedFunctionKey(Signature signature, TypeManager typeManager)
    {
        try {
            return specializedFunctionKeyCache.computeIfAbsent(signature, k -> doGetSpecializedFunctionKey(k, typeManager));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private SpecializedFunctionKey doGetSpecializedFunctionKey(Signature signature, TypeManager typeManager)
    {
        Iterable<SqlInvokedFunction> candidates = getFunctions(Optional.empty(), signature.getName());
        // First, search for generic match with exact types
        Type returnType = typeManager.getType(signature.getReturnType());
        List<TypeSignatureProvider> argumentTypeSignatureProviders = fromTypeSignatures(signature.getArgumentTypes());
        for (SqlFunction candidate : candidates) {
            Optional<BoundVariables> boundVariables = new SignatureBinder(typeManager, candidate.getSignature(), false)
                    .bindVariables(argumentTypeSignatureProviders, returnType);
            if (boundVariables.isPresent()) {
                return new SpecializedFunctionKey(candidate, boundVariables.get(), argumentTypeSignatureProviders.size());
            }
        }

        // TODO (AP) [START]: do we even need this block of code? We may be able to remove it
        // TODO: hack because there could be "type only" coercions (which aren't necessarily included as implicit casts),
        // so do a second pass allowing "type only" coercions
        List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), typeManager);
        for (SqlFunction candidate : candidates) {
            SignatureBinder binder = new SignatureBinder(typeManager, candidate.getSignature(), true);
            Optional<BoundVariables> boundVariables = binder.bindVariables(argumentTypeSignatureProviders, returnType);
            if (!boundVariables.isPresent()) {
                continue;
            }
            Signature boundSignature = applyBoundVariables(candidate.getSignature(), boundVariables.get(), argumentTypes.size());

            if (!typeManager.isTypeOnlyCoercion(typeManager.getType(boundSignature.getReturnType()), returnType)) {
                continue;
            }
            boolean nonTypeOnlyCoercion = false;
            for (int i = 0; i < argumentTypes.size(); i++) {
                Type expectedType = typeManager.getType(boundSignature.getArgumentTypes().get(i));
                if (!typeManager.isTypeOnlyCoercion(argumentTypes.get(i), expectedType)) {
                    nonTypeOnlyCoercion = true;
                    break;
                }
            }
            if (nonTypeOnlyCoercion) {
                continue;
            }

            return new SpecializedFunctionKey(candidate, boundVariables.get(), argumentTypes.size());
        }
        // TODO (AP) [END]
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    private class FunctionCollection
    {
        @GuardedBy("this")
        private final Map<QualifiedObjectName, Collection<SqlInvokedFunction>> functions = new ConcurrentHashMap<>();

        @GuardedBy("this")
        private final Map<Signature, SqlFunctionHandle> functionHandles = new ConcurrentHashMap<>();

        public synchronized Collection<SqlInvokedFunction> loadAndGetFunctionsTransactional(QualifiedObjectName functionName)
        {
            Collection<SqlInvokedFunction> functions = this.functions.computeIfAbsent(functionName, AbstractSqlInvokedFunctionNamespaceManager.this::fetchFunctions);
            functionHandles.putAll(functions.stream().collect(toImmutableMap(SqlInvokedFunction::getSignature, SqlInvokedFunction::getRequiredFunctionHandle)));
            return functions;
        }

        public synchronized FunctionHandle getFunctionHandle(Signature signature, TypeManager typeManager)
        {
            return getExactFunctionHandle(signature).orElseGet(() -> getGenericFunctionHandle(signature, typeManager));
        }

        private synchronized Optional<FunctionHandle> getExactFunctionHandle(Signature signature)
        {
            return functionHandles.get(signature) != null ? Optional.of(functionHandles.get(signature)) : Optional.empty();
        }

        // TODO (AP): Can you make the first lookup more efficient? 2nd+ lookups are efficient
        private synchronized SqlFunctionHandle getGenericFunctionHandle(Signature signature, TypeManager typeManager) throws UncheckedExecutionException
        {
            SqlInvokedFunction function = (SqlInvokedFunction) getSpecializedFunctionKey(signature, typeManager).getFunction();
            SqlFunctionHandle handle = new SqlFunctionHandle(
                    functionHandles.get(function.getSignature()).getFunctionId(),
                    functionHandles.get(function.getSignature()).getVersion(),
                    signature);

            functionHandles.put(signature, handle);
            metadataByHandle.put(handle, sqlInvokedFunctionToMetadata(function));
            return handle;
        }
    }
}
