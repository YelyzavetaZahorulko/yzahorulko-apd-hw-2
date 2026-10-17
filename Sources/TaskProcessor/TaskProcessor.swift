public struct Job: Sendable, Equatable {
    public let id: String
    public let input: Int

    public init(id: String, input: Int) {
        self.id = id
        self.input = input
    }
}

public enum JobOutcome: Sendable, Equatable {
    case succeeded(Int)
    case failed
    case cancelled
}

public struct JobResult: Sendable, Equatable {
    public let id: String
    public let outcome: JobOutcome

    public init(id: String, outcome: JobOutcome) {
        self.id = id
        self.outcome = outcome
    }
}

public enum TaskProcessorError: Error, Equatable {
    case nonPositiveMaximumConcurrency
    case duplicateID(String)
}

public actor TaskProcessor {
    private let maximumConcurrency: Int

    public init(maximumConcurrency: Int) throws {
        guard maximumConcurrency > 0 else {
            throw TaskProcessorError.nonPositiveMaximumConcurrency
        }
        self.maximumConcurrency = maximumConcurrency
    }

    public func process(
        _ jobs: [Job],
        worker: @escaping @Sendable (Job) async throws -> Int
    ) async throws -> [JobResult] {
        try await runWithExecutor(jobs) { job in
            do {
                return .succeeded(try await worker(job))
            } catch is CancellationError {
                return .cancelled
            } catch {
                return .failed
            }
        }
    }

    func runWithExecutor(
        _ jobs: [Job],
        execute: @escaping @Sendable (Job) async -> JobOutcome
    ) async throws -> [JobResult] {
        try Self.validateUniqueIDs(jobs)
        if jobs.isEmpty { return [] }
        let outcomes = await Self.run(jobs, limit: maximumConcurrency, execute: execute)
        return zip(jobs, outcomes).map { JobResult(id: $0.id, outcome: $1) }
    }

    private static func run(
        _ jobs: [Job],
        limit: Int,
        execute: @escaping @Sendable (Job) async -> JobOutcome
    ) async -> [JobOutcome] {
        var outcomes = [JobOutcome?](repeating: nil, count: jobs.count)

        await withTaskGroup(of: (Int, JobOutcome).self) { group in
            var next = 0
            var stopped = false

            while next < min(limit, jobs.count) {
                if Task.isCancelled {
                    stopped = true
                    group.cancelAll()
                    break
                }
                let index = next
                let job = jobs[index]
                group.addTask { (index, await execute(job)) }
                next += 1
            }

            for await (index, outcome) in group {
                outcomes[index] = outcome
                guard !stopped, next < jobs.count else { continue }

                if Task.isCancelled {
                    stopped = true
                    group.cancelAll()
                } else {
                    let nextIndex = next
                    let job = jobs[nextIndex]
                    group.addTask { (nextIndex, await execute(job)) }
                    next += 1
                }
            }
        }

        return outcomes.map { $0 ?? .cancelled }
    }

    private static func validateUniqueIDs(_ jobs: [Job]) throws {
        var seen = Set<String>()
        seen.reserveCapacity(jobs.count)
        for job in jobs {
            if !seen.insert(job.id).inserted {
                throw TaskProcessorError.duplicateID(job.id)
            }
        }
    }
}

public struct RetryPolicy: Sendable, Equatable {
    public let maximumAttempts: Int

    public init(maximumAttempts: Int) throws {
        guard maximumAttempts > 0 else {
            throw RetryPolicyError.nonPositiveMaximumAttempts
        }
        self.maximumAttempts = maximumAttempts
    }
}

public enum RetryPolicyError: Error, Equatable {
    case nonPositiveMaximumAttempts
}

public extension TaskProcessor {
    func process(
        _ jobs: [Job],
        retryPolicy: RetryPolicy,
        worker: @escaping @Sendable (Job, Int) async throws -> Int
    ) async throws -> [JobResult] {
        let attempts = retryPolicy.maximumAttempts
        return try await runWithExecutor(jobs) { job in
            var attempt = 1
            while true {
                do {
                    return .succeeded(try await worker(job, attempt))
                } catch is CancellationError {
                    return .cancelled
                } catch {
                    if attempt >= attempts { return .failed }
                    attempt += 1
                }
            }
        }
    }
}
