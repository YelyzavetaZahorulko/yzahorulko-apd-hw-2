import XCTest
import TaskProcessor

private actor CallCounter {
    private(set) var count = 0
    func increment() { count += 1 }
}

private struct TestError: Error {}

private actor Probe {
    private(set) var active = 0
    private(set) var peak = 0
    private(set) var started: [String] = []
    private(set) var finished: [String] = []

    func begin(_ id: String) {
        active += 1
        peak = max(peak, active)
        started.append(id)
    }
    func end(_ id: String) {
        active -= 1
        finished.append(id)
    }
}

private actor Gate {
    private var opened = Set<String>()
    private var waiters: [String: CheckedContinuation<Void, Never>] = [:]

    func wait(_ key: String) async {
        if opened.contains(key) { return }
        await withCheckedContinuation { waiters[key] = $0 }
    }
    func open(_ key: String) {
        opened.insert(key)
        waiters.removeValue(forKey: key)?.resume()
    }
}

private actor AttemptLog {
    private(set) var values: [Int] = []
    func record(_ attempt: Int) { values.append(attempt) }
}

private func waitUntil(_ condition: () async -> Bool) async -> Bool {
    for _ in 0..<5000 {
        if await condition() { return true }
        try? await Task.sleep(nanoseconds: 1_000_000)
    }
    return false
}

final class StudentTaskProcessorTests: XCTestCase {


    func testInitThrowsForZeroConcurrency() {
        XCTAssertThrowsError(try TaskProcessor(maximumConcurrency: 0)) { error in
            XCTAssertEqual(error as? TaskProcessorError, .nonPositiveMaximumConcurrency)
        }
    }

    func testInitThrowsForNegativeConcurrency() {
        XCTAssertThrowsError(try TaskProcessor(maximumConcurrency: -3)) { error in
            XCTAssertEqual(error as? TaskProcessorError, .nonPositiveMaximumConcurrency)
        }
    }

    func testInitSucceedsForPositiveConcurrency() {
        XCTAssertNoThrow(try TaskProcessor(maximumConcurrency: 1))
    }

    func testDuplicateIDThrowsFirstRepeatedIDAndStartsNoWorker() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let counter = CallCounter()
        let jobs = [
            Job(id: "a", input: 1),
            Job(id: "b", input: 2),
            Job(id: "b", input: 3),
            Job(id: "a", input: 4),
        ]
        do {
            _ = try await processor.process(jobs) { _ in
                await counter.increment()
                return 0
            }
            XCTFail("Expected duplicateID error")
        } catch {
            XCTAssertEqual(error as? TaskProcessorError, .duplicateID("b"))
        }
        let calls = await counter.count
        XCTAssertEqual(calls, 0)
    }

    func testEmptyJobsReturnsEmptyAndStartsNoWorker() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 3)
        let counter = CallCounter()
        let results = try await processor.process([]) { _ in
            await counter.increment()
            return 0
        }
        XCTAssertEqual(results, [])
        let calls = await counter.count
        XCTAssertEqual(calls, 0)
    }

    func testSuccessfulJobsReturnSucceededOutcomes() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let jobs = [Job(id: "a", input: 1), Job(id: "b", input: 2), Job(id: "c", input: 3)]
        let results = try await processor.process(jobs) { $0.input * 10 }
        XCTAssertEqual(results, [
            JobResult(id: "a", outcome: .succeeded(10)),
            JobResult(id: "b", outcome: .succeeded(20)),
            JobResult(id: "c", outcome: .succeeded(30)),
        ])
    }

    func testOutcomeMappingAndUnaffectedJobsContinue() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let jobs = [
            Job(id: "a", input: 1),
            Job(id: "b", input: 2),
            Job(id: "c", input: 3),
            Job(id: "d", input: 4),
        ]
        let results = try await processor.process(jobs) { job in
            switch job.id {
            case "b": throw TestError()
            case "c": throw CancellationError()
            default: return job.input
            }
        }
        XCTAssertEqual(results.map(\.id), ["a", "b", "c", "d"])
        XCTAssertEqual(results.map(\.outcome), [.succeeded(1), .failed, .cancelled, .succeeded(4)])
    }

    func testResultsKeepInputOrderWhenCompletionIsReversed() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 3)
        let probe = Probe()
        let gate = Gate()
        let jobs = [Job(id: "a", input: 1), Job(id: "b", input: 2), Job(id: "c", input: 3)]

        let run = Task {
            try await processor.process(jobs) { job in
                await probe.begin(job.id)
                await gate.wait(job.id)
                await probe.end(job.id)
                return job.input
            }
        }

        let allStarted = await waitUntil { await probe.started.count == 3 }
        XCTAssertTrue(allStarted)

        for id in ["c", "b", "a"] {
            await gate.open(id)
            let done = await waitUntil { await probe.finished.contains(id) }
            XCTAssertTrue(done)
        }

        let results = try await run.value
        let finishedOrder = await probe.finished
        XCTAssertEqual(finishedOrder, ["c", "b", "a"])
        XCTAssertEqual(results, [
            JobResult(id: "a", outcome: .succeeded(1)),
            JobResult(id: "b", outcome: .succeeded(2)),
            JobResult(id: "c", outcome: .succeeded(3)),
        ])
    }

    func testNeverExceedsMaximumConcurrencyAndFreedSlotStartsNextJob() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let probe = Probe()
        let gate = Gate()
        let jobs = (0..<5).map { Job(id: "j\($0)", input: $0) }

        let run = Task {
            try await processor.process(jobs) { job in
                await probe.begin(job.id)
                await gate.wait(job.id)
                await probe.end(job.id)
                return job.input
            }
        }

        let twoStarted = await waitUntil { await probe.started.count == 2 }
        XCTAssertTrue(twoStarted)
        try await Task.sleep(nanoseconds: 20_000_000)
        let startedBefore = await probe.started
        XCTAssertEqual(startedBefore, ["j0", "j1"])

        await gate.open("j0")
        let threeStarted = await waitUntil { await probe.started.count == 3 }
        XCTAssertTrue(threeStarted)
        let startedAfter = await probe.started
        XCTAssertEqual(startedAfter, ["j0", "j1", "j2"])

        for job in jobs { await gate.open(job.id) }
        let results = try await run.value

        let peak = await probe.peak
        XCTAssertLessThanOrEqual(peak, 2)
        XCTAssertEqual(results.map(\.id), jobs.map(\.id))
        XCTAssertEqual(results.map(\.outcome), (0..<5).map { JobOutcome.succeeded($0) })
    }

    func testConcurrencyLargerThanJobCountRunsEveryJobOnce() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 10)
        let counter = CallCounter()
        let jobs = [Job(id: "x", input: 7), Job(id: "y", input: 8)]
        let results = try await processor.process(jobs) { job in
            await counter.increment()
            return job.input
        }
        let calls = await counter.count
        XCTAssertEqual(calls, 2)
        XCTAssertEqual(results.map(\.outcome), [.succeeded(7), .succeeded(8)])
    }

    func testCancellationMarksQueuedJobsCancelledAndStartsNoNewWorker() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let probe = Probe()
        let jobs = (0..<5).map { Job(id: "j\($0)", input: $0) }

        let run = Task {
            try await processor.process(jobs) { job in
                await probe.begin(job.id)
                try await Task.sleep(nanoseconds: 60_000_000_000)
                await probe.end(job.id)
                return job.input
            }
        }

        let twoStarted = await waitUntil { await probe.started.count == 2 }
        XCTAssertTrue(twoStarted)

        run.cancel()
        let results = try await run.value

        let started = await probe.started
        XCTAssertEqual(started, ["j0", "j1"])
        XCTAssertEqual(results.map(\.id), jobs.map(\.id))
        XCTAssertEqual(results.map(\.outcome), Array(repeating: JobOutcome.cancelled, count: 5))
    }

    func testCompletedResultsSurviveCancellation() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let probe = Probe()
        let gate = Gate()
        let jobs = [Job(id: "a", input: 1), Job(id: "b", input: 2), Job(id: "c", input: 3)]

        let run = Task {
            try await processor.process(jobs) { job in
                await probe.begin(job.id)
                if job.id == "a" {
                    await gate.wait("a")
                    await probe.end(job.id)
                    return job.input
                }
                try await Task.sleep(nanoseconds: 60_000_000_000)
                return job.input
            }
        }

        let aStarted = await waitUntil { await probe.started.count == 1 }
        XCTAssertTrue(aStarted)

        await gate.open("a")
        let bStarted = await waitUntil { await probe.started.count == 2 }
        XCTAssertTrue(bStarted)

        run.cancel()
        let results = try await run.value

        XCTAssertEqual(results, [
            JobResult(id: "a", outcome: .succeeded(1)),
            JobResult(id: "b", outcome: .cancelled),
            JobResult(id: "c", outcome: .cancelled),
        ])
    }

    func testAlreadyCancelledCallerStartsNoWorker() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let counter = CallCounter()
        let gate = Gate()
        let jobs = [Job(id: "a", input: 1), Job(id: "b", input: 2), Job(id: "c", input: 3)]

        let run = Task {
            await gate.wait("go")
            return try await processor.process(jobs) { job in
                await counter.increment()
                return job.input
            }
        }

        run.cancel()
        await gate.open("go")
        let results = try await run.value

        let calls = await counter.count
        XCTAssertEqual(calls, 0)
        XCTAssertEqual(results.map(\.id), ["a", "b", "c"])
        XCTAssertEqual(results.map(\.outcome), Array(repeating: JobOutcome.cancelled, count: 3))
    }

    func testLimitOneRunsJobsSeriallyInInputOrder() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let probe = Probe()
        let jobs = (0..<4).map { Job(id: "s\($0)", input: $0) }

        let results = try await processor.process(jobs) { job in
            await probe.begin(job.id)
            await Task.yield()
            await probe.end(job.id)
            return job.input
        }

        let started = await probe.started
        let peak = await probe.peak
        XCTAssertEqual(started, jobs.map(\.id))
        XCTAssertEqual(peak, 1)
        XCTAssertEqual(results.map(\.outcome), (0..<4).map { JobOutcome.succeeded($0) })
    }

    func testOrdinaryFailureDoesNotStopQueuedJobs() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let counter = CallCounter()
        let jobs = [Job(id: "a", input: 1), Job(id: "b", input: 2), Job(id: "c", input: 3)]

        let results = try await processor.process(jobs) { job in
            await counter.increment()
            if job.id == "a" { throw TestError() }
            return job.input
        }

        let calls = await counter.count
        XCTAssertEqual(calls, 3)
        XCTAssertEqual(results.map(\.outcome), [.failed, .succeeded(2), .succeeded(3)])
    }

    func testWorkerIgnoringCancellationKeepsSucceededOutcome() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let probe = Probe()
        let gate = Gate()
        let jobs = [Job(id: "a", input: 1), Job(id: "b", input: 2)]

        let run = Task {
            try await processor.process(jobs) { job in
                await probe.begin(job.id)
                await gate.wait(job.id)
                await probe.end(job.id)
                return job.input
            }
        }

        let aStarted = await waitUntil { await probe.started.count == 1 }
        XCTAssertTrue(aStarted)

        run.cancel()
        await gate.open("a")
        let results = try await run.value

        let started = await probe.started
        XCTAssertEqual(started, ["a"])
        XCTAssertEqual(results, [
            JobResult(id: "a", outcome: .succeeded(1)),
            JobResult(id: "b", outcome: .cancelled),
        ])
    }

    func testProcessorCanBeReusedForSequentialInvocations() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let first = try await processor.process([Job(id: "a", input: 1)]) { $0.input }
        let second = try await processor.process([Job(id: "a", input: 5), Job(id: "b", input: 6)]) { $0.input }

        XCTAssertEqual(first, [JobResult(id: "a", outcome: .succeeded(1))])
        XCTAssertEqual(second.map(\.outcome), [.succeeded(5), .succeeded(6)])
    }

    func testRetryPolicyRejectsNonPositiveAttempts() {
        XCTAssertThrowsError(try RetryPolicy(maximumAttempts: 0)) {
            XCTAssertEqual($0 as? RetryPolicyError, .nonPositiveMaximumAttempts)
        }
        XCTAssertThrowsError(try RetryPolicy(maximumAttempts: -1)) {
            XCTAssertEqual($0 as? RetryPolicyError, .nonPositiveMaximumAttempts)
        }
    }

    func testRetryRecoversBeforeLimitWithExactAttemptNumbers() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let attempts = AttemptLog()
        let policy = try RetryPolicy(maximumAttempts: 3)

        let results = try await processor.process([Job(id: "a", input: 9)], retryPolicy: policy) { job, attempt in
            await attempts.record(attempt)
            if attempt < 3 { throw TestError() }
            return job.input
        }

        let seen = await attempts.values
        XCTAssertEqual(seen, [1, 2, 3])
        XCTAssertEqual(results, [JobResult(id: "a", outcome: .succeeded(9))])
    }

    func testRetryFinalFailureAfterMaximumAttempts() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let attempts = AttemptLog()
        let policy = try RetryPolicy(maximumAttempts: 2)

        let results = try await processor.process([Job(id: "a", input: 1)], retryPolicy: policy) { _, attempt in
            await attempts.record(attempt)
            throw TestError()
        }

        let seen = await attempts.values
        XCTAssertEqual(seen, [1, 2])
        XCTAssertEqual(results, [JobResult(id: "a", outcome: .failed)])
    }

    func testRetryNeverRetriesCancellationError() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 1)
        let attempts = AttemptLog()
        let policy = try RetryPolicy(maximumAttempts: 5)

        let results = try await processor.process([Job(id: "a", input: 1)], retryPolicy: policy) { _, attempt in
            await attempts.record(attempt)
            throw CancellationError()
        }

        let seen = await attempts.values
        XCTAssertEqual(seen, [1])
        XCTAssertEqual(results, [JobResult(id: "a", outcome: .cancelled)])
    }

    func testRetryKeepsInputOrderAndJobLevelConcurrencyLimit() async throws {
        let processor = try TaskProcessor(maximumConcurrency: 2)
        let probe = Probe()
        let policy = try RetryPolicy(maximumAttempts: 3)
        let jobs = (0..<5).map { Job(id: "r\($0)", input: $0) }

        let results = try await processor.process(jobs, retryPolicy: policy) { job, attempt in
            await probe.begin(job.id)
            await Task.yield()
            await probe.end(job.id)
            if attempt < 2 { throw TestError() }
            return job.input
        }

        let peak = await probe.peak
        XCTAssertLessThanOrEqual(peak, 2)
        XCTAssertEqual(results.map(\.id), jobs.map(\.id))
        XCTAssertEqual(results.map(\.outcome), (0..<5).map { JobOutcome.succeeded($0) })
    }
}
