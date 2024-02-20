package org.shashkun;

import org.tinkoff.ApplicationStatusResponse;
import org.tinkoff.Client;
import org.tinkoff.Handler;
import org.tinkoff.Response;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class HandlerImpl implements Handler {
    private final Client serviceClient;
    private final ExecutorService executorService;
    private final Duration operationTimeout;

    public HandlerImpl(Client serviceClient, int poolSize, Duration operationTimeout) {
        this(serviceClient, Executors.newFixedThreadPool(poolSize), operationTimeout);
    }

    public HandlerImpl(Client serviceClient, ExecutorService executorService, Duration operationTimeout) {
        this.serviceClient = serviceClient;
        this.executorService = executorService;
        this.operationTimeout = operationTimeout;
    }

    @Override
    public ApplicationStatusResponse performOperation(String id) {
        List<Callable<ApplicationStatusResponse>> tasks = new ArrayList<>();
        AtomicReference<ApplicationStatusResponse> appResponseRef = new AtomicReference<>();
        AtomicInteger retriesCount = new AtomicInteger();
        tasks.add(() -> execute(() -> serviceClient.getApplicationStatus1(id), retriesCount, appResponseRef));
        tasks.add(() -> execute(() -> serviceClient.getApplicationStatus2(id), retriesCount, appResponseRef));
        try {
            return executorService.invokeAny(tasks, operationTimeout.toSeconds(), TimeUnit.SECONDS);
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof ClientFailureException failureException) {
                // return failure from client service
                return failureException.getFailure();
            }
            // return failure from Handler
            return new ApplicationStatusResponse.Failure(null, 0);
        } catch (TimeoutException | InterruptedException ex) {
            ApplicationStatusResponse.Failure failure = new ApplicationStatusResponse.Failure(null, 0);
            // explicitly set the value to prevent any next client service executions,
            // because of no assumption how org.tinkoff.Client works with InterruptedException
            appResponseRef.set(failure);
            return failure;
        }
    }

    private ApplicationStatusResponse execute(Supplier<Response> serviceExecutor, AtomicInteger retriesCount, AtomicReference<ApplicationStatusResponse> appResponseRef) throws ClientFailureException, InterruptedException {
        while (appResponseRef.get() == null) {
            Response clientResponse = serviceExecutor.get();
            switch (clientResponse) {
                case Response.Failure failure ->
                        throw new ClientFailureException(new ApplicationStatusResponse.Failure(null, retriesCount.get()));
                case Response.RetryAfter retry -> {
                    retriesCount.incrementAndGet();
                    wait(retry.delay());
                }
                case Response.Success successResponse ->
                        appResponseRef.compareAndSet(null, new ApplicationStatusResponse.Success(successResponse.applicationId(), successResponse.applicationStatus()));
            }
        }
        return appResponseRef.get();
    }

    private static void wait(Duration delay) throws InterruptedException {
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException("Interrupt while waiting, delay: " + delay);
        }
    }

    private static class ClientFailureException extends Exception {
        private final ApplicationStatusResponse.Failure failure;

        public ClientFailureException(ApplicationStatusResponse.Failure failure) {
            this.failure = failure;
        }

        public ApplicationStatusResponse.Failure getFailure() {
            return failure;
        }
    }
}