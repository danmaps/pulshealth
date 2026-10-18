import Foundation
import HealthKit
import os

/// Live, per-type view of sync progress: persisted counters plus in-flight activity.
public struct TypeSyncStatus: Identifiable, Sendable {
    public var id: String { state.identifier }
    public var descriptor: HealthTypeDescriptor
    public var state: TypeSyncState
    public var activity: Activity
    /// Samples/second over the current backfill run, if one is active.
    public var currentRate: Double?
    /// Estimated seconds remaining for this type's backfill, if estimable.
    public var estimatedSecondsRemaining: Double?

    /// Until when the automatic paths skip this type, after the server
    /// refused its upload in a way a retry cannot change (`state.lastError`
    /// says how); nil when it is not cooling down. Sync Now tries anyway.
    public var cooldownUntil: Date? {
        state.isCoolingDown() ? state.cooldownUntil : nil
    }

    public enum Activity: String, Sendable {
        case idle
        case backfilling
        case syncing
        case failed
    }
}

/// Orchestrates all syncing: authorization, parallel backfill, observer-driven
/// incremental sync, and stats. One instance per app.
public actor HealthSyncEngine {
    public let store: SyncStateStore
    public let eventLog: SyncEventLog
    /// Durable per-wake telemetry: when the app got execution time, why, and what
    /// it did with it. Powers the Background Activity screen and the diagnostics export.
    public let wakeLog: WakeLog

    // Internal (not private) so the AggregateSync extension shares the same
    // health store, transport, and overlap guards.
    let healthStore = HKHealthStore()
    var transport: SyncTransport?
    /// Read-side client for /v1/stats and the reconciliation endpoints.
    // Internal so isolated-session tests can exercise the complete GET lifetime.
    var apiClient: ServerAPIClient?
    private var observerQuery: HKObserverQuery?
    /// Types (and "agg:<uuid>" aggregate-config keys) with a sync currently
    /// running (prevents overlapping runs per type/config).
    var activeSyncs: Set<String> = []
    // Held across store awaits: no run may capture a half-applied destination.
    var configurationMutationInProgress = false
    /// Keys asked to sync while a run was active — re-run when the active one finishes.
    var pendingResync: Set<String> = []
    // Internal (not private) so the ActivitySummarySync extension can mark the
    // rings type busy while it runs (same rationale as activeSyncs above).
    var activities: [String: TypeSyncStatus.Activity] = [:]
    // Internal (not private) so the MergedSync extension can keep per-type
    // backfill progress in step while pages from several types share an upload.
    var backfillRuns: [String: BackfillRun] = [:]
    private var changeContinuations: [UUID: AsyncStream<Void>.Continuation] = [:]
    private var databaseLockedContinuations: [UUID: AsyncStream<Void>.Continuation] = [:]
    private let logger = Logger(subsystem: PulsLog.subsystem, category: "engine")
    /// Samples HealthKit returned that `SampleMapper` could not convert, per
    /// type, over this engine's lifetime. The event log already carries a
    /// warning for each such page, but a log line is prose: a caller that has
    /// to *report* the loss — the on-device export, whose result must never
    /// read "0 samples" for a type that had thousands — needs the number, and
    /// should not have to parse it back out of a message.
    private(set) var unmappableSampleCounts: [String: Int] = [:]

    /// Observer deliveries gathered but not yet run. HealthKit does not hand out
    /// one callback per change — production telemetry recorded bursts of up to
    /// 93 callbacks inside five seconds — so callbacks are accumulated here and
    /// run as one wake instead of 93. See `enqueueObserverUpdate`.
    private var pendingObserverTypes: Set<String> = []
    private var pendingObserverCompletions: [ObserverCompletion] = []
    private var observerFlushTask: Task<Void, Never>?
    /// Cached from the configuration so the burst path never has to await the
    /// state actor between mutating the two pending collections above.
    private var observerCoalesceWindow: TimeInterval = 2.0

    /// Runs an observer wake under background time: `BackgroundExecution.run`
    /// in the app. Replaced only by tests, which drive the expiration handler
    /// themselves (the package's test runner has no `UIApplication`).
    var backgroundExecution: BackgroundExecutionRunner = { name, onExpiration, work in
        await BackgroundExecution.run(name, onExpiration: onExpiration, work)
    }
    /// Stands in for `ProtectedData.isAvailable` when set — tests only, for
    /// the same reason. Nil in the app.
    var protectedDataOverride: Bool?

    /// HealthKit's completion handler isn't statically Sendable but is
    /// documented safe to call from any thread; box it to cross the actor hop.
    struct ObserverCompletion: @unchecked Sendable {
        let finish: () -> Void
    }

    struct BackfillRun {
        var startedAt: ContinuousClock.Instant
        var samplesThisRun: Int = 0
        var estimatedTotal: Int?
    }

    /// Whether a type still backfilling sends its recent window ahead of the
    /// sweep (`RecentSampleWindow`). Right for anything that feeds a server;
    /// wrong for a destination where a sample sent twice is a duplicate row.
    let recentWindowFirst: Bool

    /// Exclusive upper bound on what this engine reads, or nil for none.
    /// Raw sweeps read samples whose start is before it (`MergedSync.queryPage`,
    /// which every anchored page — backfill, merged and the recent-window
    /// stream — goes through); aggregate runs compute buckets that end at or
    /// before it (`runAggregateSync` clamps its "now" to it, and the window's
    /// floor drops the bucket that straddles it); the rings query ends at the
    /// local day before it; and the route/stream phases follow only workouts
    /// that started before it.
    ///
    /// **Set only by `HealthExporter`'s throwaway engine.** The app's engine
    /// must never pass one: a bounded sweep drains at that date and marks
    /// every backfill complete, so its sync would stop there for good.
    let readEnd: Date?

    /// iOS 27 limited history access: what `refreshReadableHistory` last
    /// found (type identifier → earliest readable date, limited types only)
    /// and when, so the automatic paths can ask at most every
    /// `readableHistoryInterval`.
    var readableHistory: [String: Date] = [:]
    var readableHistoryCheckedAt: ContinuousClock.Instant?

    var lastReadableAccessPolicy: SyncAccessPolicy?
    let accessPolicy: @Sendable () async -> SyncAccessPolicy

    public init(
        store: SyncStateStore? = nil, eventLog: SyncEventLog? = nil, wakeLog: WakeLog? = nil,
        recentWindowFirst: Bool = true, readEnd: Date? = nil,
        accessPolicy: @escaping @Sendable () async -> SyncAccessPolicy = { .unrestricted }
    ) {
        self.store = store ?? SyncStateStore()
        self.eventLog = eventLog ?? SyncEventLog()
        self.wakeLog = wakeLog ?? WakeLog()
        self.recentWindowFirst = recentWindowFirst
        self.accessPolicy = accessPolicy
        self.readEnd = readEnd
    }

    // MARK: - Wake lifecycle

    /// Open a wake: record it durably and log it. Run the wake's work inside
    /// `WakeScope.$current.withValue(context)` so every nested sync task and the
    /// transport see it, then call `finishWake`. Used by every entry point that
    /// gives the engine execution time (observer, background tasks, foreground,
    /// manual).
    public func beginWake(_ trigger: WakeTrigger, detail: String? = nil) async -> WakeContext {
        let context = WakeContext(trigger: trigger)
        await registerWake(context, detail: detail)
        return context
    }

    /// Register a pre-created wake context. Background-task entry points build the
    /// `WakeContext` synchronously (so both the work task and the expiration
    /// handler can close over the same id) and register it here once the task runs.
    public func registerWake(_ context: WakeContext, detail: String? = nil) async {
        await wakeLog.begin(context, detail: detail)
        await eventLog.log(.info, "Wake [\(context.trigger.rawValue)] started" + (detail.map { " — \($0)" } ?? ""))
    }

    /// Close a wake opened with `beginWake`, recording its outcome.
    public func finishWake(_ context: WakeContext, outcome: WakeRecord.Outcome = .completed) async {
        let finished = await wakeLog.finish(wakeID: context.id, outcome: outcome)
        if let finished {
            await eventLog.log(.info, "Wake [\(context.trigger.rawValue)] \(outcome.rawValue) — \(finished.summary)")
        }
        notifyChanged()
    }

    /// Attribute one uploaded batch to the wake currently in scope, if any. No-op
    /// for work not running under a wake (e.g. reconciliation, tests).
    func reportWakeBatch(type: String, samples: Int, deletions: Int, bytes: Int) async {
        guard let wake = WakeScope.current else { return }
        await wakeLog.record(
            wakeID: wake.id, type: type, samples: samples, deletions: deletions, bytes: bytes)
    }

    // MARK: - Configuration

    /// Apply settings and pair the receiver, retaining a separate complete
    /// progress snapshot for each server/user identity. A new identity starts
    /// fresh; a saved identity resumes its own progress. The confirmation flag
    /// remains source-compatible but never permits copying another receiver's
    /// anchors. Returns false if a run is active or persistence fails.
    @discardableResult
    public func configure(
        _ config: SyncConfiguration, confirmServerIdentity: Bool = false, clearAuthToken: Bool = false
    ) async -> Bool {
        guard activeSyncs.isEmpty, !configurationMutationInProgress else { return false }
        configurationMutationInProgress = true
        defer { configurationMutationInProgress = false }
        do {
            try await store.applySavedConfiguration(config, clearAuthToken: clearAuthToken)
        } catch {
            await eventLog.log(.error, "Database settings could not be saved: \(error.localizedDescription)")
            return false
        }
        await destinationDidChange()
        return true
    }

    /// Replace the bearer token alone (`SyncStateStore.setAuthToken`).
    /// Returns false while a sync or configuration change is in flight.
    @discardableResult
    public func setAuthToken(_ token: String) async -> Bool {
        guard activeSyncs.isEmpty, !configurationMutationInProgress else { return false }
        configurationMutationInProgress = true
        defer { configurationMutationInProgress = false }
        var config = await store.configuration
        config.authToken = token
        do { try await store.applySavedConfiguration(config) }
        catch { return false }
        buildTransport(from: await store.configuration)
        notifyChanged()
        return true
    }

    /// Delete the bearer token (`SyncStateStore.clearAuthToken`): the user
    /// emptied the token field or disconnected the database. Syncing stops
    /// until a token is applied again. Returns false while a sync or
    /// configuration change is in flight; a caller must retry after it finishes.
    @discardableResult
    public func clearAuthToken() async -> Bool {
        guard activeSyncs.isEmpty, !configurationMutationInProgress else { return false }
        configurationMutationInProgress = true
        defer { configurationMutationInProgress = false }
        let config = await store.configuration
        do { try await store.applySavedConfiguration(config, clearAuthToken: true) }
        catch { return false }
        buildTransport(from: await store.configuration)
        notifyChanged()
        return true
    }

    /// Switches progress, configuration and credentials together. No captured
    /// old transport can still upload once this method succeeds.
    public func switchDestination(id: UUID) async throws {
        guard activeSyncs.isEmpty, !configurationMutationInProgress else { throw SavedDestinationError.busy }
        configurationMutationInProgress = true
        defer { configurationMutationInProgress = false }
        try await store.switchDestination(id: id)
        await destinationDidChange()
    }

    /// Forgets local progress and credentials only; never deletes server data.
    public func unpairDestination(id: UUID) async throws {
        guard activeSyncs.isEmpty, !configurationMutationInProgress else { throw SavedDestinationError.busy }
        configurationMutationInProgress = true
        defer { configurationMutationInProgress = false }
        try await store.unpairDestination(id: id)
        await destinationDidChange()
    }

    private func destinationDidChange() async {
        let config = await store.configuration
        observerCoalesceWindow = max(0, config.observerCoalesceWindow)
        activities.removeAll()
        backfillRuns.removeAll()
        pendingResync.removeAll()
        backfillExpectedUntil = nil
        readableHistory.removeAll()
        readableHistoryCheckedAt = nil
        buildTransport(from: config)
        notifyChanged()
    }

    /// Whether HealthKit can be read right now. False means the device is
    /// locked: every query would fail with `errorDatabaseInaccessible`, so
    /// callers should skip rather than burn a wake on ~80 doomed queries.
    public func isHealthDataAccessible() async -> Bool {
        if let protectedDataOverride { return protectedDataOverride }
        return await ProtectedData.isAvailable
    }

    private func buildTransport(from config: SyncConfiguration) {
        if let url = config.serverURL, let token = config.authToken {
            transport = HTTPSyncTransport(baseURL: url, authToken: token, userID: config.userID)
            apiClient = ServerAPIClient(baseURL: url, authToken: token, userID: config.userID)
        } else {
            transport = nil
            apiClient = nil
        }
    }

    /// The transport lives only in memory, but the server URL and token persist
    /// in the state store — rebuild it on demand so cold launches (background
    /// task wake-ups especially) can sync without waiting for `configure`.
    func ensureTransport() async {
        guard let configurationUse = beginConfigurationUse() else { return }
        defer { activeSyncs.remove(configurationUse) }
        // Never clobber a transport injected via `setTransport` (tests/benchmark).
        guard transport == nil else { return }
        buildTransport(from: await store.configuration)
    }

    /// Server-side per-type aggregates, for comparing against device counters.
    public func serverStats() async throws -> [TypeServerStats] {
        guard let configurationUse = beginConfigurationUse() else { throw SyncError.configurationBusy }
        defer { activeSyncs.remove(configurationUse) }
        await ensureTransport()
        guard let apiClient else { throw TransportError.notConfigured }
        return try await apiClient.stats()
    }

    /// What the configured server advertises on `GET /v1/capabilities`. A
    /// server without the endpoint throws `TransportError.serverError` 404/405;
    /// callers treat that as "no features" (see `ConnectionTester`).
    public func serverCapabilities() async throws -> ServerCapabilities {
        guard let configurationUse = beginConfigurationUse() else { throw SyncError.configurationBusy }
        defer { activeSyncs.remove(configurationUse) }
        await ensureTransport()
        guard let apiClient else { throw TransportError.notConfigured }
        return try await apiClient.capabilities()
    }

    /// Test/benchmark hook: swap the upload destination.
    public func setTransport(_ transport: SyncTransport?) {
        self.transport = transport
    }

    /// Read-authorization object types for the given catalog identifiers: the
    /// bulk-readable sample types plus `HKActivitySummaryType` (an HKObjectType,
    /// not an HKSampleType, so it can't ride the sample-type path) when the
    /// activity-rings type is among them.
    private func readAuthorizationTypes(
        for identifiers: [String], includeRoutes: Bool
    ) -> Set<HKObjectType> {
        var read = Set<HKObjectType>(HealthTypeCatalog.bulkReadAuthorizationSampleTypes(
            for: identifiers, includeWorkoutRoutes: includeRoutes
        ).map { $0 as HKObjectType })
        if identifiers.contains(where: HealthTypeCatalog.isActivitySummary) {
            read.insert(HKObjectType.activitySummaryType())
        }
        // HealthKit derives HRV from beat-to-beat intervals, so it refuses to
        // authorize the heartbeat series unless HRV SDNN read auth rides along:
        // a heartbeat-only read set raises an uncatchable NSInvalidArgumentException
        // ("Authorization for …HeartRateVariabilitySDNN should also be requested
        // when requesting authorization to read …HeartbeatSeries") — and not just
        // from requestAuthorization but from statusForAuthorizationRequest too,
        // which the per-type authorizationNeeded(for:) probe hits. Pair them in
        // every auth/status path. (The full-catalog path already carries SDNN as a
        // normal quantity type; this guarantees it for scoped/per-type requests.)
        if identifiers.contains(HealthTypeCatalog.heartbeatSeriesIdentifier) {
            read.insert(HKQuantityType(.heartRateVariabilitySDNN))
        }
        return read
    }

    /// Ask for read access to the whole catalog. `.declined` is the user's
    /// Don't Allow on iOS 27's history page, not an error — see
    /// `HealthAccessRequestOutcome`.
    @discardableResult
    public func requestAuthorization() async throws -> HealthAccessRequestOutcome {
        guard HKHealthStore.isHealthDataAvailable() else {
            throw SyncError.healthDataUnavailable
        }
        let config = await store.configuration
        let types = readAuthorizationTypes(
            for: HealthTypeCatalog.all.map(\.identifier),
            includeRoutes: config.includeWorkoutRoutes)
        return try await presentAuthorization(read: types, describing: "\(types.count) types")
    }

    /// The one call to HealthKit's permission sheet. Its second page on
    /// iOS 27 ("How much data would you like to share?") throws
    /// `errorAuthorizationDenied` for Don't Allow, where the first page's
    /// Don't Allow returns normally — both are the user's answer, so both
    /// return, and only a real failure throws.
    private func presentAuthorization(
        read types: Set<HKObjectType>, describing what: String
    ) async throws -> HealthAccessRequestOutcome {
        do {
            try await healthStore.requestAuthorization(toShare: [], read: types)
        } catch {
            guard let outcome = HealthAccessRequestOutcome.classify(error) else { throw error }
            await eventLog.log(
                .info,
                "Health access for \(what): Don't Allow on the history page — nothing granted; these types stay unread until access is allowed")
            return outcome
        }
        await eventLog.log(.info, "HealthKit read authorization requested for \(what)")
        return .answered
    }

    /// Medications use HealthKit's per-object authorization: the user picks which
    /// medications the app may read. Call when the medication dose type is enabled.
    @available(iOS 26.0, *)
    public func requestMedicationAuthorization() async throws {
        for type in HealthTypeCatalog.perObjectReadAuthorizationObjectTypes
        where type.requiresPerObjectAuthorization() {
            try await healthStore.requestPerObjectReadAuthorization(for: type, predicate: nil)
        }
        await eventLog.log(.info, "Per-object medication authorization requested")
    }

    /// Request read authorization for the given catalog types only (plus workout
    /// routes when workouts are included). Used to prompt for just-enabled types
    /// without dragging the whole catalog into the sheet. `.declined` is the
    /// user's Don't Allow on iOS 27's history page, which leaves the types
    /// undetermined; it is an answer, not an error.
    @discardableResult
    public func requestAuthorization(for identifiers: [String]) async throws -> HealthAccessRequestOutcome {
        guard HKHealthStore.isHealthDataAvailable() else {
            throw SyncError.healthDataUnavailable
        }
        let includeRoutes = await store.configuration.includeWorkoutRoutes
        let types = readAuthorizationTypes(for: identifiers, includeRoutes: includeRoutes)
        guard !types.isEmpty else { return .answered }
        return try await presentAuthorization(read: types, describing: "\(types.count) enabled types")
    }

    /// True when iOS would still show the permission sheet for some catalog type —
    /// i.e. at least one type's read authorization is not yet determined. Queries
    /// against such types throw `errorAuthorizationNotDetermined`, so the app
    /// should offer to (re-)request access.
    public func authorizationNeeded() async -> Bool {
        guard HKHealthStore.isHealthDataAvailable() else { return false }
        // Per-object-authorization types (medications) are disallowed in bulk
        // authorization APIs — passing one raises an ObjC exception, not a Swift
        // error. Same filter as requestAuthorization().
        let config = await store.configuration
        let types = readAuthorizationTypes(
            for: HealthTypeCatalog.all.map(\.identifier),
            includeRoutes: config.includeWorkoutRoutes)
        let status = try? await healthStore.statusForAuthorizationRequest(toShare: [], read: types)
        return status == .shouldRequest
    }

    /// Like `authorizationNeeded()`, but scoped to the given catalog types (plus
    /// workout routes when workouts are included).
    public func authorizationNeeded(for identifiers: [String]) async -> Bool {
        guard HKHealthStore.isHealthDataAvailable() else { return false }
        let includeRoutes = await store.configuration.includeWorkoutRoutes
        let types = readAuthorizationTypes(for: identifiers, includeRoutes: includeRoutes)
        guard !types.isEmpty else { return false }
        let status = try? await healthStore.statusForAuthorizationRequest(toShare: [], read: types)
        return status == .shouldRequest
    }

    // MARK: - Status for the app

    /// A pass may snapshot settings before it claims individual types, or
    /// release all children between phases. Keep its destination stable for
    /// that entire lifetime, not just while an individual upload is active.
    func beginConfigurationUse() -> String? {
        guard !configurationMutationInProgress else { return nil }
        let key = "configuration-use:\(UUID())"
        activeSyncs.insert(key)
        return key
    }

    /// Whether a run is in flight for `key`: a raw type identifier, the rings
    /// key, `"agg:<uuid>"`, or `"workout-enrich:<kind>"`.
    public func isSyncing(_ key: String) -> Bool { activeSyncs.contains(key) }

    /// Reset a type's anchor and counters (or the rings watermark). Refused,
    /// returning false, while that type is syncing: the running pass captured
    /// its state at start, and its next `recordUploadedBatch` would re-persist
    /// the *current* anchor over the reset with `backfillComplete == false` —
    /// the "backfill" then drains immediately, marks itself complete, and
    /// history is never re-exported, with no error anywhere. Holding the busy
    /// key for the duration also parks any run that starts meanwhile.
    @discardableResult
    public func resetType(_ identifier: String) async -> Bool {
        let isRings = HealthTypeCatalog.isActivitySummary(identifier)
        let key = isRings ? "activitySummary" : identifier
        guard !configurationMutationInProgress, !activeSyncs.contains(key) else { return false }
        activeSyncs.insert(key)
        defer { activeSyncs.remove(key) }
        if isRings {
            await store.resetActivitySummary()
        } else {
            await store.resetType(identifier)
        }
        notifyChanged()
        return true
    }

    /// Clear an aggregate config's watermark. Refused while it is computing,
    /// for the same reason as `resetType`: `recordAggregateUpload` takes
    /// `max(existing, new)`, so a reset under a running series degrades
    /// "recompute everything" into an ordinary incremental run.
    @discardableResult
    public func resetAggregate(configID: UUID) async -> Bool {
        let key = "agg:\(configID.uuidString)"
        guard !configurationMutationInProgress, !activeSyncs.contains(key) else { return false }
        activeSyncs.insert(key)
        defer { activeSyncs.remove(key) }
        await store.resetAggregate(configID: configID)
        notifyChanged()
        return true
    }

    /// Reset every anchor and watermark. Refused while anything is running.
    @discardableResult
    public func resetAll() async -> Bool {
        guard activeSyncs.isEmpty, !configurationMutationInProgress else { return false }
        configurationMutationInProgress = true
        defer { configurationMutationInProgress = false }
        await store.resetAll()
        notifyChanged()
        return true
    }

    /// Whether applying `config` would point the stored progress at a different
    /// server or user (see `ServerIdentity`). Non-nil means: ask the user
    /// before `configure` — start fresh (`resetAll()` then `configure(_:
    /// confirmServerIdentity: true)`) or keep progress (`configure` with the
    /// confirmation alone).
    public func serverIdentityChange(applying config: SyncConfiguration) async -> ServerIdentityChange? {
        await store.serverIdentityChange(applying: config)
    }

    /// A mismatch between the persisted configuration and the recorded server
    /// identity — left behind when a change was applied but never confirmed.
    public func pendingServerIdentityChange() async -> ServerIdentityChange? {
        await store.pendingServerIdentityChange()
    }

    public func snapshot() async -> [TypeSyncStatus] {
        let config = await store.configuration
        var out: [TypeSyncStatus] = []
        for id in config.enabledTypes.sorted() {
            guard let descriptor = HealthTypeCatalog.descriptor(for: id) else { continue }
            // Activity summaries keep their progress in a dedicated singleton
            // watermark, not a per-type TypeSyncState — project it onto the same
            // shape so the per-type dashboard row shows real counters.
            if HealthTypeCatalog.isActivitySummary(id) {
                let s = await store.activitySummaryState
                var state = TypeSyncState(identifier: id)
                state.backfillComplete = s.computedThrough != nil
                state.totalSamplesExported = s.totalDaysUploaded
                state.totalBatchesUploaded = s.totalBatchesUploaded
                state.totalBytesUploaded = s.totalBytesUploaded
                state.latestExported = s.computedThrough
                state.lastSyncAt = s.lastComputedAt
                state.lastError = s.lastError
                state.lastErrorAt = s.lastErrorAt
                state.readableSince = s.readableSince
                out.append(TypeSyncStatus(
                    descriptor: descriptor, state: state,
                    activity: activities[id] ?? (s.lastError != nil ? .failed : .idle)))
                continue
            }
            let state = await store.state(for: id)
            var status = TypeSyncStatus(
                descriptor: descriptor, state: state,
                activity: activities[id] ?? (state.lastError != nil ? .failed : .idle)
            )
            if let run = backfillRuns[id] {
                let elapsed = (ContinuousClock.now - run.startedAt).seconds
                if elapsed > 1, run.samplesThisRun > 0 {
                    let rate = Double(run.samplesThisRun) / elapsed
                    status.currentRate = rate
                    if let total = run.estimatedTotal, total > run.samplesThisRun {
                        status.estimatedSecondsRemaining = Double(total - run.samplesThisRun) / rate
                    }
                }
            }
            out.append(status)
        }
        return out
    }

    /// Fires whenever any type's status changes; the UI re-reads `snapshot()`.
    public func changes() -> AsyncStream<Void> {
        let id = UUID()
        return AsyncStream { continuation in
            changeContinuations[id] = continuation
            continuation.onTermination = { [weak self] _ in
                Task { await self?.removeChangeContinuation(id) }
            }
        }
    }

    private func removeChangeContinuation(_ id: UUID) {
        changeContinuations[id] = nil
    }

    func notifyChanged() {
        for c in changeContinuations.values { c.yield(()) }
    }

    /// Fires each time a pass meets `errorDatabaseInaccessible` — HealthKit's
    /// store locked under it. Every pass treats that as a non-failure (the
    /// type goes idle, progress untouched) and only logs it, so this is the
    /// typed signal for a caller that has to act on it: the continued
    /// backfill stops once the device confirms it is locked
    /// (`BackgroundSyncScheduler`).
    public func healthDatabaseLockedSignals() -> AsyncStream<Void> {
        let id = UUID()
        return AsyncStream { continuation in
            databaseLockedContinuations[id] = continuation
            continuation.onTermination = { [weak self] _ in
                Task { await self?.removeDatabaseLockedContinuation(id) }
            }
        }
    }

    private func removeDatabaseLockedContinuation(_ id: UUID) {
        databaseLockedContinuations[id] = nil
    }

    /// Called from every `PassFailure.databaseLocked` branch.
    func noteHealthDatabaseLocked() {
        for c in databaseLockedContinuations.values { c.yield(()) }
    }

    /// Both sweeps (`runSync` here, `MergedSync`) call this beside their
    /// "Dropped N of M samples" warning.
    func noteUnmappableSamples(_ count: Int, type identifier: String) {
        unmappableSampleCounts[identifier, default: 0] += count
    }

    // MARK: - Backfill

    /// Run a full sync: activity summaries, a bounded recent aggregate window,
    /// every enabled type `maxConcurrentTypes` at a time, every enabled aggregate
    /// config, then workout enrichment. Each type pages independently and
    /// persists its anchor after every uploaded batch, so this is fully resumable
    /// at batch granularity, and every phase boundary is a safe place to be
    /// interrupted.
    ///
    /// The first two phases are there because a first backfill is long and the
    /// things worth looking at soonest are the cheapest to produce.
    public func syncAllEnabled(reason: SyncReason = .backfill) async {
        guard let configurationUse = beginConfigurationUse() else { return }
        defer { activeSyncs.remove(configurationUse) }
        let config = await store.configuration
        // Activity summaries aren't anchored/sample-based — they ride their own
        // HKActivitySummaryQuery path, so keep them out of syncTypes.
        let sampleIDs = config.enabledTypes.sorted().filter { !HealthTypeCatalog.isActivitySummary($0) }
        let activitySummaryEnabled = config.enabledTypes.contains(HealthTypeCatalog.activitySummaryIdentifier)
        let hasAggregates = config.aggregates.contains(where: \.enabled)
        guard !sampleIDs.isEmpty || hasAggregates || activitySummaryEnabled else {
            await eventLog.log(.warn, "syncAllEnabled called with no enabled types")
            return
        }
        // Bail before the sweep rather than during it. iOS runs the catch-up
        // BGProcessingTask when the device is idle — overnight, locked — where
        // HealthKit is unreadable, so this used to walk ~80 types and log a
        // warning for every one of them, several times a night, for nothing.
        // Anchors and watermarks are untouched; the next unlocked run catches up.
        guard await isHealthDataAccessible() else {
            await eventLog.log(
                .info,
                "Device locked — HealthKit is unreadable; skipping \(reason.rawValue) sync of \(sampleIDs.count) types")
            return
        }
        // Before anything is claimed: a type whose Health access widened is
        // re-swept, which resets its anchor, and a type this run holds could
        // not be. Rate-limited — the app asks on every foreground itself.
        await refreshReadableHistory()
        // A backfill claims its raw types now, before the two cheap phases
        // below, not when phase 3 reaches them. The app registers its observer
        // query moments before a first backfill starts, HealthKit answers the
        // registration with a delivery, and two seconds later that wake's
        // merged pass claimed every type nobody held yet — so the backfill's
        // four per-type pipelines found them taken and the whole first sync
        // ran one upload at a time. Seen on a reinstall on 2026-09-26: the
        // backfill proper moved heart rate and a handful of small types, and
        // every other type went the slow way.
        let claimed = reason == .incremental
            ? [] : claimTypes(HealthTypeCatalog.backfillOrder(sampleIDs))
        if reason != .incremental { backfillExpectedUntil = nil }
        var sweepStarted = false
        defer { if !sweepStarted { for id in claimed { activeSyncs.remove(id) } } }
        // Phase 1: the rings. One row per day and no dependency on any other
        // phase, so this is seconds of work — but it used to run after the raw
        // sweep, which on a first backfill meant the dashboard had no activity
        // data until every type had drained. Cheapest useful thing there is;
        // put it first.
        if activitySummaryEnabled {
            await syncActivitySummary(reason: reason)           // phase 1
        }
        // Phase 2: a bounded recent window over aggregates that have never been
        // computed. The server's daily views join `aggregate_series`, and only an
        // aggregate line creates a row there, so before this phase existed the
        // viewer's daily charts stayed empty through the whole raw sweep and then
        // through a full aggregate pass that runs oldest-first. A few dozen
        // buckets per config fixes that. It moves no watermark, so phase 4 still
        // recomputes each series from the start date; it self-gates to configs
        // that have never been computed, so it is a no-op after the first run.
        if hasAggregates {
            await syncRecentAggregates(reason: reason)          // phase 2
        }
        if reason == .incremental {                             // phase 3 (workouts: basic only)
            await syncTypes(sampleIDs, reason: reason)
        } else {
            sweepStarted = true
            await sweep(claimed, reason: reason)
        }
        // Background time ran out: stop here rather than start phases that
        // would only read HealthKit after the assertion is gone.
        guard !Task.isCancelled else { return }
        if hasAggregates {
            await syncAllAggregates(reason: reason)             // phase 4
        }
        // Phases 5 & 6 (LAST): enrich the now-uploaded workout rows — routes
        // first, then the heavier intra-workout streams. Only when workouts are
        // raw-synced (the basic rows must exist server-side for the UUID join);
        // each phase also self-gates on its config flag.
        if sampleIDs.contains(HealthTypeCatalog.workoutIdentifier) {
            await syncWorkoutRoutes(reason: reason)             // phase 5 (routes)
            await syncWorkoutStreams(reason: reason)            // phase 6 (streams)
        }
    }

    /// Run a full sync of the given types, `maxConcurrentTypes` at a time.
    ///
    /// Incremental runs take the merged path (see `MergedSync`): their pages are
    /// small — a median of 7 samples — so the per-request round trip, not the
    /// data, is the cost. Backfill keeps the per-type path, where pages are full
    /// and running four independent type pipelines overlaps query and upload
    /// better than a fetch-all-then-upload-all pass would.
    ///
    /// The sweep runs in `HealthTypeCatalog.backfillOrder` rather than in the
    /// caller's order: heaviest type first so the critical path holds a slot
    /// from the start, then cheapest-first so the long tail of once-a-day types
    /// is on the server within the opening minutes. The types are independent,
    /// so this only changes what finishes when.
    public func syncTypes(_ ids: [String], reason: SyncReason = .backfill) async {
        guard let configurationUse = beginConfigurationUse() else { return }
        defer { activeSyncs.remove(configurationUse) }
        guard !ids.isEmpty else { return }
        if reason == .incremental {
            await eventLog.log(.info, "Starting incremental sync of \(ids.count) types (merged batches)")
            await syncTypesMerged(ids, reason: reason)
            await eventLog.log(.info, "Completed incremental sync of \(ids.count) types")
            notifyChanged()
            return
        }
        await refreshReadableHistory()
        let claimed = claimTypes(HealthTypeCatalog.backfillOrder(ids))
        backfillExpectedUntil = nil
        await sweep(claimed, reason: reason)
    }

    /// Claim every type in `ids` that no run holds yet, keeping their order,
    /// and mark the rest for a follow-up run by whoever holds them — so data an
    /// observer reported mid-run is not missed.
    ///
    /// All of a sweep's types are claimed at once, up front. Claiming each one
    /// only as a pipeline slot reached it left the queued ones free for any
    /// merged pass that came along meanwhile — an observer wake, above all —
    /// and the slot then found its type taken and skipped it.
    func claimTypes(_ ids: [String]) -> [String] {
        guard !configurationMutationInProgress else { return [] }
        var claimed: [String] = []
        for id in ids {
            if activeSyncs.contains(id) {
                pendingResync.insert(id)
            } else {
                activeSyncs.insert(id)
                claimed.append(id)
            }
        }
        return claimed
    }

    /// The per-type path over types the caller has already claimed,
    /// `maxConcurrentTypes` at a time. Each type is released the moment its own
    /// run ends rather than when the whole sweep does, so a type that is done
    /// goes back to ordinary observer-driven syncing while the rest continue.
    func sweep(_ claimed: [String], reason: SyncReason) async {
        guard !claimed.isEmpty else { return }
        let config = await store.configuration
        await eventLog.log(.info, "Starting \(reason.rawValue) sync of \(claimed.count) types (\(config.maxConcurrentTypes) concurrent)")
        let signpostID = PulsLog.signposter.makeSignpostID()
        let signpostState = PulsLog.signposter.beginInterval("syncAll", id: signpostID)
        defer { PulsLog.signposter.endInterval("syncAll", signpostState) }

        await sendRecentWindows(claimed, reason: reason)

        let started = await withTaskGroup(of: Void.self, returning: Int.self) { group in
            var iterator = claimed.makeIterator()
            var inFlight = 0
            var started = 0
            func addNext(_ group: inout TaskGroup<Void>) {
                // Once cancelled, start nothing more: a child started now would
                // read a page of HealthKit before it noticed.
                guard let id = iterator.next(),
                      group.addTaskUnlessCancelled(operation: {
                          await self.runClaimed(type: id, reason: reason)
                      })
                else { return }
                inFlight += 1
                started += 1
            }
            for _ in 0..<max(1, config.maxConcurrentTypes) { addNext(&group) }
            while inFlight > 0 {
                await group.next()
                inFlight -= 1
                addNext(&group)
            }
            return started
        }
        // A run releases its own type. One that never started — the sweep was
        // cancelled first — still holds its claim, and would keep every later
        // run of that type waiting forever.
        for id in claimed.dropFirst(started) { activeSyncs.remove(id) }
        await eventLog.log(.info, "Completed \(reason.rawValue) sync of \(claimed.count) types")
        notifyChanged()
    }

    /// Sync one type: anchored-query pages until drained, uploading each page.
    public func sync(type identifier: String, reason: SyncReason = .incremental) async {
        guard let configurationUse = beginConfigurationUse() else { return }
        defer { activeSyncs.remove(configurationUse) }
        await refreshReadableHistory()
        guard claimTypes([identifier]) == [identifier] else { return }
        await sendRecentWindows([identifier], reason: reason)
        await runClaimed(type: identifier, reason: reason)
        if identifier == HealthTypeCatalog.workoutIdentifier, !Task.isCancelled {
            await syncWorkoutRoutes(reason: reason)
            await syncWorkoutStreams(reason: reason)
        }
    }

    /// One type's run, repeated while observers asked for more during it, then
    /// the claim is released. The caller must hold the claim.
    private func runClaimed(type identifier: String, reason: SyncReason) async {
        defer { activeSyncs.remove(identifier) }
        await drainClaimedSync(type: identifier, reason: reason)
    }

    /// Drain queued observer work without releasing a claim owned by a caller
    /// that also uses it for reconciliation's snapshot and deletion lifetime.
    private func drainClaimedSync(type identifier: String, reason: SyncReason) async {
        var nextReason = reason
        repeat {
            await runSync(type: identifier, reason: nextReason)
            nextReason = .incremental
        } while !Task.isCancelled && pendingResync.remove(identifier) != nil
    }

    /// Upload the settings-backed user identity without waiting for a workout.
    /// This is intentionally a standalone, profile-only batch: profile edits are
    /// independent of HealthKit sample availability and should take effect when
    /// the user taps Save & Apply.
    public func syncProfile(reason: SyncReason = .manual) async throws {
        let key = "profile:\(UUID())"
        guard !configurationMutationInProgress else { throw SyncError.configurationBusy }
        activeSyncs.insert(key)
        defer { activeSyncs.remove(key) }
        await ensureTransport()
        guard let transport else { throw TransportError.notConfigured }
        let config = await store.configuration
        let profile = config.userProfilePayload
        let batch = SyncBatch(
            deviceID: store.deviceID,
            type: HealthTypeCatalog.workoutIdentifier,
            reason: reason,
            samples: [],
            deletions: [],
            profile: profile
        )
        let result = try await transport.upload(batch)
        await reportWakeBatch(type: "profile", samples: 0, deletions: 0, bytes: result.bytesSent)
        await eventLog.log(.info, "User profile uploaded")
    }

    /// The page after `page`, or nil when `page` was short — a short raw page
    /// is HealthKit saying it has nothing more.
    private func readAhead(
        _ identifier: String, after page: MergedPage, config: SyncConfiguration
    ) async throws -> MergedPage? {
        guard !page.drained else { return nil }
        return try await queryPage(
            identifier, anchor: page.newAnchor, start: config.startDate, config: config)
    }

    private func runSync(type identifier: String, reason: SyncReason) async {
        guard await allowsSync(types: [identifier]) else { return }
        guard let descriptor = HealthTypeCatalog.descriptor(for: identifier),
              descriptor.sampleType != nil else {
            await eventLog.log(.error, type: identifier, "Unknown type identifier")
            return
        }
        await ensureTransport()
        guard let transport else {
            await eventLog.log(.error, type: identifier, "No transport configured — set server URL and token")
            return
        }

        let config = await store.configuration
        var initialState = await store.state(for: identifier)
        // The server refused this type's last upload in a way a retry cannot
        // change; its anchor has not moved, so this run would only re-send
        // the same page. A run someone asked for tries anyway.
        if initialState.isCoolingDown(), !Self.bypassesCooldown(reason) {
            await eventLog.log(
                .debug, type: identifier,
                "Skipped: cooling down after the server refused an upload (\(initialState.terminalFailures ?? 0) in a row)")
            return
        }
        let startAnchor = await storedAnchor(initialState.anchorData, of: identifier, pass: .main)
        if startAnchor == nil, initialState.anchorData != nil {
            // The anchor would not decode and the type was started over;
            // run from the state that left (backfill reopened).
            initialState = await store.state(for: identifier)
        }
        let isBackfill = !initialState.backfillComplete
        activities[identifier] = isBackfill ? .backfilling : .syncing
        if isBackfill, backfillRuns[identifier] == nil {
            let days = max(1, Calendar.current.dateComponents(
                [.day], from: config.startDate, to: Date()).day ?? 1)
            backfillRuns[identifier] = BackfillRun(
                startedAt: .now,
                estimatedTotal: descriptor.estimatedSamplesPerDay * days
            )
        }
        notifyChanged()

        let signpostID = PulsLog.signposter.makeSignpostID()
        let interval = PulsLog.signposter.beginInterval("syncType", id: signpostID, "\(identifier)")
        defer { PulsLog.signposter.endInterval("syncType", interval) }

        let runStart = ContinuousClock.now
        var pages = 0
        var totalSamples = 0
        var totalDeletions = 0
        var droppedAnything = false

        do {
            try Task.checkCancellation()
            var next: MergedPage? = try await queryPage(
                identifier, anchor: startAnchor, start: config.startDate, config: config)

            while let page = next {
                try Task.checkCancellation()
                if page.isRawEmpty {
                    // Genuinely drained. Persist the final anchor so
                    // observer-triggered syncs start from here. Clear any stale
                    // error: the query just executed successfully, so a prior
                    // auth-not-determined error (e.g. recorded before the grant) no
                    // longer holds. Without this, a 0-sample type never reaches
                    // recordUploadedBatch — the only other place lastError is
                    // cleared — and the error sticks forever.
                    await store.update(identifier) {
                        $0.anchorData = page.newAnchorData
                        $0.lastSyncAt = Date()
                        $0.lastError = nil
                        $0.clearCooldown()
                    }
                    break
                }

                // Samples HealthKit returned that SampleMapper could not convert.
                // map() returns nil when the quantity is not compatible with the
                // catalog's unitString — a whole-type property — so one wrong unit
                // makes EVERY sample of that type unmappable. Counting mapped
                // samples here (rather than raw ones) is how such a type reported
                // "0 samples, drained, backfill complete" with no error and no
                // counter anywhere. `page.drained` has always compared raw
                // counts; this branch now agrees with it.
                if page.dropped > 0 {
                    droppedAnything = true
                    noteUnmappableSamples(page.dropped, type: identifier)
                    await eventLog.log(
                        .warn, type: identifier,
                        "Dropped \(page.dropped) of \(page.rawCount) samples that could not be mapped — check this type's unitString in HealthTypeCatalog"
                    )
                }

                // Read the next page while this one uploads. Only the in-memory
                // cursor runs ahead: the persisted anchor still advances after
                // each ack and in order, and an upload that throws leaves this
                // scope — cancelling the read with it — before its page's anchor
                // is recorded, so the next run re-reads from the last acked page.
                async let following = readAhead(identifier, after: page, config: config)

                if page.isEmpty {
                    // HealthKit returned a page, but nothing on it survived mapping.
                    // There is nothing to upload, so advance past it — re-querying
                    // the same unmappable samples forever is worse — but do NOT
                    // treat it as drained: whether more pages remain is the raw
                    // short-page question `readAhead` asks, not a mapped one.
                    await store.update(identifier) {
                        $0.anchorData = page.newAnchorData
                        $0.lastSyncAt = Date()
                    }
                    pages += 1
                    next = try await following
                    continue
                }

                let batch = SyncBatch(
                    deviceID: store.deviceID,
                    type: identifier,
                    reason: reason,
                    samples: page.samples,
                    deletions: page.deletions,
                    routes: page.enrichment.routes,
                    series: page.enrichment.series,
                    profile: page.enrichment.profile
                )
                let uploadResult = try await transport.upload(batch)

                let dates = page.samples.map(\.start)
                let latency: TimeInterval? = (reason == .incremental)
                    ? page.samples.map { Date().timeIntervalSince($0.end) }.min()
                    : nil
                await store.recordUploadedBatch(
                    identifier: identifier,
                    newAnchorData: page.newAnchorData,
                    samples: page.samples.count,
                    deletions: page.deletions.count,
                    bytes: uploadResult.bytesSent,
                    sampleDateRange: dates.isEmpty ? nil
                        : (dates.min()! ... dates.max()!),
                    duration: page.queryDuration + uploadResult.duration,
                    latency: latency,
                    receipt: uploadResult.receipt
                )
                await reportWakeBatch(
                    type: identifier, samples: page.samples.count,
                    deletions: page.deletions.count, bytes: uploadResult.bytesSent)

                pages += 1
                totalSamples += page.samples.count
                totalDeletions += page.deletions.count
                backfillRuns[identifier]?.samplesThisRun += page.samples.count
                await eventLog.log(
                    .debug, type: identifier,
                    "Page \(pages): \(page.samples.count) samples\(uploadResult.receipt?.sampleOutcome.map { " (\($0))" } ?? ""), \(page.deletions.count) deletions — query \(String(format: "%.2f", page.queryDuration))s, upload \(String(format: "%.2f", uploadResult.duration))s (\(uploadResult.bytesSent) B)"
                )
                notifyChanged()

                // Nil after a short page: HealthKit had nothing more.
                next = try await following
            }

            // A type that dropped anything is never marked complete (MergedSync
            // holds the same line): "backfill complete" beside a type whose
            // samples never left the phone is the silent failure the raw-count
            // rule exists to prevent. The run itself is over either way.
            if isBackfill {
                if !droppedAnything { await store.markBackfillComplete(identifier) }
                backfillRuns[identifier] = nil
            }
            let elapsed = (ContinuousClock.now - runStart).seconds
            if totalSamples + totalDeletions > 0 {
                let rate = Double(totalSamples) / max(elapsed, 0.001)
                await eventLog.log(
                    .info, type: identifier,
                    "\(reason.rawValue) sync done: \(totalSamples) samples, \(totalDeletions) deletions in \(String(format: "%.1f", elapsed))s (\(Int(rate))/s, \(pages) pages)"
                )
            }
            activities[identifier] = .idle
        } catch {
            switch PassFailure(error) {
            case .authorizationNotDetermined:
                activities[identifier] = .failed
                backfillRuns[identifier] = nil
                await store.recordError(identifier: identifier, error: SyncError.authorizationNotDetermined)
                await eventLog.log(.error, type: identifier, "Health access not determined — tap Grant Health Access on the Explore tab")
            case .databaseLocked:
                noteHealthDatabaseLocked()
                // Device locked: the Health DB is Protected-Unless-Open and relocks
                // ~10 min after lock. Expected during background runs — not a failure;
                // the anchor is untouched and the next wake/foreground catches up.
                activities[identifier] = .idle
                await eventLog.log(.warn, type: identifier, "Health database locked (device locked) — will retry on next wake")
            case .cancelled:
                // Background time expired (BG task expiration handler cancels the
                // sync task). Not a failure: the anchor sits at the last acked page
                // and the next wake resumes from there. Recording it as an error
                // would show every routine expiration as a failed type.
                activities[identifier] = .idle
                await eventLog.log(.debug, type: identifier, "Sync cancelled — will resume on next wake")
            case .failed(let error):
                activities[identifier] = .failed
                backfillRuns[identifier] = nil
                await store.recordError(identifier: identifier, error: error)
                await eventLog.log(.error, type: identifier, "Sync failed: \(error)")
            }
        }
        notifyChanged()
    }

    /// Secondary data attached to a workout batch: GPS routes, intra-workout
    /// quantity-series streams, and the user profile (DOB/sex for HR zones).
    struct WorkoutEnrichment {
        var routes: [RoutePayload] = []
        var series: [WorkoutSeriesPayload] = []
        var profile: ProfilePayload?
    }

    /// Runs the follow-up queries series-style samples need (beats, voltages,
    /// routes, streams, effort scores) and returns the workout-batch enrichment.
    /// `includeEnhanced` gates the enhanced workout data (series streams, per-type
    /// detail stats, events, sub-activities, profile); when false, only the basic
    /// workout payload is sent.
    ///
    /// `deferEnrichment` skips the two expensive per-workout queries — GPS routes
    /// and intra-workout series — so phase-1 workout batches stay lightweight; the
    /// dedicated route/stream phases fetch and upload them afterwards. Heartbeat
    /// and ECG enrichment (which decorate the sample row itself, not separate
    /// lines) and the cheap, settings-derived profile are unaffected.
    // Internal so MergedSync can run the same enrichment before packing.
    func enrich(
        _ samples: inout [SyncSample], from hkSamples: [HKSample],
        descriptor: HealthTypeDescriptor, includeRoutes: Bool, includeEnhanced: Bool,
        userProfile: ProfilePayload, deferEnrichment: Bool = false
    ) async throws -> WorkoutEnrichment {
        let enricher = SeriesEnricher(healthStore: healthStore)
        switch descriptor.kind {
        case .heartbeatSeries:
            let byUUID = Dictionary(
                hkSamples.compactMap { ($0 as? HKHeartbeatSeriesSample).map { ($0.uuid, $0) } },
                uniquingKeysWith: { a, _ in a }
            )
            var unreadable = 0
            var lastError: Error?
            for i in samples.indices {
                guard let series = byUUID[samples[i].uuid] else { continue }
                do {
                    samples[i].heartbeats = try await enricher.heartbeats(for: series)
                } catch {
                    // A locked store or cancellation fails the page (anchor
                    // untouched, retried next wake). Anything else is one
                    // unreadable series; failing the page for it would re-query
                    // and re-fail the same page on every wake forever, so the
                    // row goes up with the empty trace the mapper initialised.
                    if SeriesEnricher.isPhaseAbortingError(error) { throw error }
                    unreadable += 1
                    lastError = error
                }
            }
            if unreadable > 0, let lastError {
                // Counted, not named: the event log is persisted and exported,
                // and must not carry sample UUIDs.
                await eventLog.log(
                    .warn, type: descriptor.identifier,
                    "\(unreadable) of \(samples.count) heartbeat series unreadable (\(lastError)) — uploading without beats")
            }
            return WorkoutEnrichment()
        case .ecg:
            let byUUID = Dictionary(
                hkSamples.compactMap { ($0 as? HKElectrocardiogram).map { ($0.uuid, $0) } },
                uniquingKeysWith: { a, _ in a }
            )
            var unreadable = 0
            var lastError: Error?
            for i in samples.indices {
                guard let ecg = byUUID[samples[i].uuid] else { continue }
                do {
                    samples[i].ecg?.voltagesUV = try await enricher.voltagesUV(for: ecg)
                } catch {
                    if SeriesEnricher.isPhaseAbortingError(error) { throw error }
                    unreadable += 1
                    lastError = error
                }
            }
            if unreadable > 0, let lastError {
                await eventLog.log(
                    .warn, type: descriptor.identifier,
                    "\(unreadable) of \(samples.count) ECG voltage traces unreadable (\(lastError)) — uploading without the trace")
            }
            return WorkoutEnrichment()
        case .workout:
            let byUUID = Dictionary(
                hkSamples.compactMap { ($0 as? HKWorkout).map { ($0.uuid, $0) } },
                uniquingKeysWith: { a, _ in a }
            )
            var enrichment = WorkoutEnrichment()
            for i in samples.indices {
                guard let workout = byUUID[samples[i].uuid] else { continue }
                // Routes are fetched primarily in the dedicated route phase
                // (`deferEnrichment: true` in the raw sweep). When inline enrichment is
                // requested (`deferEnrichment: false`), never swallow route errors silently:
                // phase-aborting errors propagate, and per-workout route errors are
                // recorded in `SyncStateStore` so the route phase retries them.
                if includeRoutes, !deferEnrichment, Self.mayHaveWorkoutRoute(workout.workoutActivityType) {
                    let isIndoor = WorkoutRouteCandidate.extractIndoorMetadata(from: workout.metadata)
                    if isIndoor == true {
                        let now = Date()
                        await store.recordWorkoutRouteStatus(WorkoutRouteSyncRecord(
                            workoutUUID: workout.uuid,
                            activityTypeRaw: workout.workoutActivityType.rawValue,
                            workoutStart: workout.startDate,
                            workoutEnd: workout.endDate,
                            isExplicitlyIndoor: true,
                            status: .indoorWorkout,
                            firstSeenAt: now,
                            lastAttemptAt: now
                        ))
                    } else {
                        do {
                            enrichment.routes += try await enricher.routePayloads(for: workout)
                        } catch {
                            if SeriesEnricher.isPhaseAbortingError(error) { throw error }
                            let now = Date()
                            let existing = await store.workoutRouteRecord(for: workout.uuid)
                            let attempts = (existing?.attemptCount ?? 0) + 1
                            let phase = (error as? WorkoutRouteReadError)?.phase ?? "raw_enrich"
                            await store.recordWorkoutRouteStatus(WorkoutRouteSyncRecord(
                                workoutUUID: workout.uuid,
                                activityTypeRaw: workout.workoutActivityType.rawValue,
                                workoutStart: workout.startDate,
                                workoutEnd: workout.endDate,
                                isExplicitlyIndoor: isIndoor,
                                status: .readFailed,
                                attemptCount: attempts,
                                firstSeenAt: existing?.firstSeenAt ?? now,
                                lastAttemptAt: now,
                                nextRetryAt: now.addingTimeInterval(WorkoutRouteSyncRecord.retryDelay(forAttempt: attempts)),
                                lastErrorPhase: phase,
                                lastErrorMessage: error.localizedDescription
                            ))
                            await eventLog.log(
                                .warn, type: descriptor.identifier,
                                "Inline workout route read failed (\(phase)) for workout \(workout.uuid): \(error) — queued for route retry")
                        }
                    }
                }
                if includeEnhanced {
                    // Full intra-workout curves (HR/power/cadence/speed/…), best-effort.
                    // Deferred in the raw sweep (the stream phase owns the fetch); the
                    // enhanced fields baked into the row by the mapper still ship now.
                    if !deferEnrichment {
                        enrichment.series += try await enricher.seriesPayloads(for: workout)
                    }
                } else if var detail = samples[i].workout {
                    // Enhanced capture off: drop the enhanced fields the mapper
                    // always builds, leaving the basic payload.
                    detail.statisticsDetail = nil
                    detail.events = nil
                    detail.activities = nil
                    samples[i].workout = detail
                }
                if #available(iOS 18.0, *) {
                    // Effort scores predate enhanced data — keep merging them into
                    // the flat stats regardless of the enhanced toggle.
                    let effort = await enricher.effortScores(for: workout)
                    if !effort.isEmpty, var detail = samples[i].workout {
                        detail.statistics = (detail.statistics ?? [:])
                            .merging(effort) { existing, _ in existing }
                        if includeEnhanced {
                            var sd = detail.statisticsDetail ?? [:]
                            for (id, value) in effort where sd[id] == nil {
                                sd[id] = WorkoutStat(avg: value, max: value)
                            }
                            detail.statisticsDetail = sd
                        }
                        samples[i].workout = detail
                    }
                }
            }
            // The user's identity/characteristics come from settings (the User
            // page), not HealthKit, so the profile line matches what the user
            // configured. Sent only with enhanced workout data.
            if includeEnhanced, !userProfile.isEmpty { enrichment.profile = userProfile }
            return enrichment
        case .quantity, .category, .stateOfMind, .medicationDose, .activitySummary:
            return WorkoutEnrichment()
        }
    }

    // MARK: - Reconciliation

    /// Compare per-month UUID digests against the server and repair drift:
    /// re-upload samples the server is missing, send deletions for orphans the
    /// device no longer has (HealthKit purges deletion tombstones, so observer
    /// syncs alone can miss deletes).
    ///
    /// Except from a month HealthKit returned nothing for while the server
    /// has rows. Read access that is off — None in Settings, or never
    /// granted — looks exactly like that: every query answers empty, with no
    /// error, and `authorizationStatus(for:)` speaks only for writes. So
    /// such a month keeps its server rows (`ReconcileDigest.orphanVerdict`),
    /// unless iOS 27 has just listed the type as readable from a date
    /// (`ReadableLimit.isConfirmed`), the one proof of access there is; and
    /// a run that found nothing anywhere throws `reconciliationUnreadable`
    /// rather than record itself as in sync.
    public func reconcile(type identifier: String) async throws -> ReconciliationReport {
        try await withReconciliationClaim(identifier) {
            try await self.reconcileClaimed(type: identifier)
        }
    }

    /// The snapshot and its orphan deletions must exclude normal uploads for
    /// this type. An observer arriving meanwhile queues a pass for this owner.
    func withReconciliationClaim<T: Sendable>(
        _ identifier: String, operation: @Sendable () async throws -> T
    ) async throws -> T {
        guard !configurationMutationInProgress else { throw SyncError.configurationBusy }
        guard !activeSyncs.contains(identifier) else { throw SyncError.typeBusy(identifier) }
        activeSyncs.insert(identifier)
        defer { activeSyncs.remove(identifier) }
        let outcome: Result<T, Error>
        do { outcome = .success(try await operation()) }
        catch { outcome = .failure(error) }
        if !Task.isCancelled, pendingResync.remove(identifier) != nil {
            await sendRecentWindows([identifier], reason: .incremental)
            await drainClaimedSync(type: identifier, reason: .incremental)
        }
        return try outcome.get()
    }

    private func reconcileClaimed(type identifier: String) async throws -> ReconciliationReport {
        guard await allowsSync(types: [identifier]) else { throw SyncError.accessRestricted }
        guard let descriptor = HealthTypeCatalog.descriptor(for: identifier),
              let sampleType = descriptor.sampleType else {
            throw SyncError.unknownType(identifier)
        }
        guard [.quantity, .category, .workout].contains(descriptor.kind) else {
            throw SyncError.reconciliationUnsupported(identifier)
        }
        await ensureTransport()
        guard let transport, let apiClient else { throw TransportError.notConfigured }

        let config = await store.configuration
        let now = Date()
        var report = ReconciliationReport(type: identifier)
        var repairedWorkoutSamples = false

        // Every server UUID the device does not return is deleted below, and
        // under iOS 27's limited history access HealthKit returns nothing
        // older than the type's earliest readable date. So the comparison
        // starts there, never before — a month the device cannot read would
        // otherwise come back as every one of its server rows "orphaned".
        // When HealthKit cannot say, the date the sync last ran under stands
        // in; with neither, nothing is compared.
        let readable = await readableLimit(
            for: identifier, recorded: await store.state(for: identifier).readableSince)
        if ReadableHistory.isSupported, !readable.isFresh, readable.since == nil {
            throw SyncError.readableHistoryUnknown(descriptor.displayName)
        }
        let from = ReadableHistory.reconcileStart(syncStart: config.startDate, readableSince: readable.since)
        if from > config.startDate { report.readableSince = from }
        await eventLog.log(
            .info, type: identifier,
            "Reconciliation started" + (from > config.startDate
                ? " — from \(from.formatted(date: .abbreviated, time: .omitted)), the earliest Health data iOS lets PulsHealth read"
                : ""))

        let serverWindows = try await apiClient.digests(
            type: identifier, from: from, to: now)
        let serverByWindow = Dictionary(
            serverWindows.map { ($0.window, $0) }, uniquingKeysWith: { a, _ in a })
        var localTotal = 0
        var serverTotal: Int64 = 0

        for window in ReconcileDigest.monthWindows(from: from, to: now) {
            try Task.checkCancellation()
            // UUIDs only, read a page at a time: a Watch heart-rate month is
            // hundreds of thousands of samples, far too many to hold as
            // `HKSample`s. The few the server turns out to be missing are
            // fetched again by UUID below.
            let localUUIDs = try await reconciliationUUIDs(of: sampleType, in: window)
            let server = serverByWindow[window.monthStart]
            let serverRows = server?.rows ?? 0
            report.windowsChecked += 1
            localTotal += localUUIDs.count
            serverTotal += serverRows

            if localUUIDs.isEmpty && serverRows == 0 { continue }
            if let server, server.rows == Int64(localUUIDs.count),
               server.digest == ReconcileDigest.hexDigest(of: localUUIDs) {
                continue
            }
            // Nothing on the device, rows on the server: a type the app may
            // not read answers exactly so, and deleting would wipe its server
            // copy. The month is left as it is — there is nothing to
            // re-upload either — unless iOS 27 vouches for read access.
            if ReconcileDigest.orphanVerdict(
                localCount: localUUIDs.count, serverRows: serverRows,
                readAccessConfirmed: readable.isConfirmed) == .withhold {
                report.windowsUnverified += 1
                report.orphanDeletionsWithheld += Int(serverRows)
                await eventLog.log(
                    .warn, type: identifier,
                    "Not reconciled \(Self.windowLabel(window.monthStart)): Health returned no samples where the database has \(serverRows); kept them, since a type whose Health access is off reads the same way"
                )
                continue
            }
            report.windowsMismatched += 1

            let serverUUIDs = try await apiClient.uuids(
                type: identifier, from: window.start, to: window.end)
            let missing = Array(localUUIDs.subtracting(serverUUIDs))
            let deletions = serverUUIDs.subtracting(localUUIDs)
                .map { SyncDeletion(uuid: $0, type: identifier) }
            guard !missing.isEmpty || !deletions.isEmpty else { continue }

            // Chunk by the configured batch size so a badly drifted month doesn't
            // become one giant upload — and fetch, map and enrich only the
            // chunk about to go, so no more than one batch of samples is held.
            var pendingMissing = missing[...]
            var pendingDeletions = deletions[...]
            var reuploaded = 0
            repeat {
                let chunk = Array(pendingMissing.prefix(config.batchSize))
                pendingMissing = pendingMissing.dropFirst(config.batchSize)
                let samples = try await reconciliationSamples(
                    chunk, of: sampleType, descriptor: descriptor, config: config)
                let chunkDeletions = Array(pendingDeletions.prefix(config.batchSize))
                pendingDeletions = pendingDeletions.dropFirst(config.batchSize)
                // A chunk whose samples were all deleted meanwhile, or would
                // not map, has nothing to send unless deletions ride along.
                guard !samples.isEmpty || !chunkDeletions.isEmpty else { continue }
                let batch = SyncBatch(
                    deviceID: store.deviceID,
                    type: identifier,
                    reason: .reconciliation,
                    samples: samples,
                    deletions: chunkDeletions
                )
                _ = try await transport.upload(batch)
                reuploaded += samples.count
            } while !pendingMissing.isEmpty || !pendingDeletions.isEmpty

            report.samplesReuploaded += reuploaded
            report.orphanDeletionsSent += deletions.count
            if descriptor.kind == .workout, reuploaded > 0 {
                repairedWorkoutSamples = true
            }
            await eventLog.log(
                .warn, type: identifier,
                "Reconciled \(Self.windowLabel(window.monthStart)): +\(reuploaded) samples, -\(deletions.count) orphans"
            )
        }

        // Nothing readable anywhere while the server has rows: most likely
        // the type's Health access is off. Nothing was sent (no samples to
        // re-upload, every deletion withheld), so say so rather than record
        // the run — "in sync" would be the opposite of the truth.
        if ReconcileDigest.looksUnreadable(
            localTotal: localTotal, serverTotal: serverTotal, readAccessConfirmed: readable.isConfirmed) {
            let error = SyncError.reconciliationUnreadable(descriptor.displayName, serverRows: serverTotal)
            await eventLog.log(.error, type: identifier, "Reconciliation stopped: \(error.localizedDescription)")
            throw error
        }

        // A repaired workout may be older than both enrichment phases' normal
        // lookback and high-water marks. Force enabled phases through a complete,
        // point-budgeted pass. Requests remain pending across an already-active
        // phase and start at its next guarded boundary.
        if repairedWorkoutSamples {
            var forcedKinds: [WorkoutEnrichmentKind] = []
            if config.includeWorkoutRoutes { forcedKinds.append(.routes) }
            if config.includeWorkoutEnhancedData { forcedKinds.append(.streams) }

            // Persist every enabled phase before starting either one. In
            // particular, termination during a route pass must not lose the
            // stream pass that reconciliation also requested.
            await store.requestForcedWorkoutEnrichmentFullRecompute(Set(forcedKinds))
            for kind in forcedKinds {
                await runGuardedWorkoutEnrichment(
                    kind, reason: .reconciliation, queueResyncWhenActive: false)
            }
        }

        await store.recordReconciliation(identifier: identifier, summary: report.summary)
        await eventLog.log(.info, type: identifier, "Reconciliation finished: \(report.summary)")
        notifyChanged()
        return report
    }

    /// How many samples one reconciliation read holds at a time.
    static let reconciliationPageSize = 5_000

    /// Every UUID of `sampleType` starting in `window`, read with an anchored
    /// query from a nil anchor — HealthKit's own cursor — `reconciliationPageSize`
    /// at a time, so only UUIDs outlive a page. The same set the old
    /// unlimited sample query produced: the same predicate, every page until
    /// a short one.
    private func reconciliationUUIDs(
        of sampleType: HKSampleType, in window: ReconcileDigest.MonthWindow
    ) async throws -> Set<UUID> {
        let predicate = HKSamplePredicate<HKSample>.sample(
            type: sampleType,
            predicate: HKQuery.predicateForSamples(
                withStart: window.start, end: window.end, options: .strictStartDate
            )
        )
        var uuids = Set<UUID>()
        var anchor: HKQueryAnchor?
        while true {
            try Task.checkCancellation()
            let page = try await HKAnchoredObjectQueryDescriptor(
                predicates: [predicate], anchor: anchor, limit: Self.reconciliationPageSize
            ).result(for: healthStore)
            for sample in page.addedSamples { uuids.insert(sample.uuid) }
            anchor = page.newAnchor
            // Raw count, deletions included, exactly as `queryPage` judges a
            // short page: fewer than asked for means HealthKit has no more.
            if page.addedSamples.count + page.deletedObjects.count < Self.reconciliationPageSize {
                return uuids
            }
        }
    }

    /// The samples behind `uuids` — the ones the server is missing — mapped
    /// and given the same enrichment the raw sweep gives them. A sample
    /// deleted since the UUIDs were read is simply absent.
    private func reconciliationSamples(
        _ uuids: [UUID], of sampleType: HKSampleType, descriptor: HealthTypeDescriptor,
        config: SyncConfiguration
    ) async throws -> [SyncSample] {
        guard !uuids.isEmpty else { return [] }
        let predicate = HKSamplePredicate<HKSample>.sample(
            type: sampleType, predicate: HKQuery.predicateForObjects(with: Set(uuids)))
        let hkSamples = try await HKSampleQueryDescriptor(
            predicates: [predicate], sortDescriptors: []
        ).result(for: healthStore)
        var samples = hkSamples.compactMap { SampleMapper.map($0, descriptor: descriptor) }
        _ = try await enrich(
            &samples, from: hkSamples, descriptor: descriptor,
            includeRoutes: config.includeWorkoutRoutes,
            includeEnhanced: config.includeWorkoutEnhancedData,
            userProfile: config.userProfilePayload,
            deferEnrichment: true
        )
        return samples
    }

    private static func windowLabel(_ date: Date) -> String {
        let comps = ReconcileDigest.utcCalendar.dateComponents([.year, .month], from: date)
        return String(format: "%04d-%02d", comps.year ?? 0, comps.month ?? 0)
    }

    // MARK: - Real-time observation

    /// Register one multi-type observer query + per-type background delivery for
    /// every observed type (raw-sync enabled ∪ referenced by an enabled aggregate
    /// config). One query for all types (instead of one per type) so a single
    /// background wake batches every change since the last wake — each wake
    /// counts against the system's delivery budget.
    ///
    /// Call this on every app launch (in `application(_:didFinishLaunchingWithOptions:)`
    /// or App init) — iOS relaunches the app in the background when new data arrives
    /// and the observer must already be registered when the update is delivered.
    /// Map every observed `HKSampleType` to the catalog identifier its delivery
    /// should wake. When workouts and workout routes are enabled, `HKSeriesType.workoutRoute()`
    /// is observed alongside `HKWorkoutType` and mapped to `HKWorkoutTypeIdentifier`
    /// so GPS routes that finish transferring from Apple Watch after the workout
    /// row itself trigger an observer wake and upload in the background.
    nonisolated static func observedSampleTypes(for config: SyncConfiguration) -> [HKSampleType: String] {
        var typeToIdentifier: [HKSampleType: String] = [:]
        for id in config.observedTypeIdentifiers {
            guard !HealthTypeCatalog.usesPerObjectAuthorization(id) else { continue }
            if let sampleType = HealthTypeCatalog.descriptor(for: id)?.sampleType {
                typeToIdentifier[sampleType] = id
            }
        }
        if config.enabledTypes.contains(HealthTypeCatalog.workoutIdentifier),
           config.includeWorkoutRoutes {
            typeToIdentifier[HKSeriesType.workoutRoute()] = HealthTypeCatalog.workoutIdentifier
        }
        return typeToIdentifier
    }

    public func startObserving() async {
        stopObserving()

        let config = await store.configuration
        let typeToIdentifier = Self.observedSampleTypes(for: config)
        guard !typeToIdentifier.isEmpty else { return }
        let queryDescriptors = typeToIdentifier.keys.map {
            HKQueryDescriptor(sampleType: $0, predicate: nil)
        }
        let typeMap = typeToIdentifier

        observerCoalesceWindow = max(0, config.observerCoalesceWindow)

        let query = HKObserverQuery(queryDescriptors: queryDescriptors) {
            [weak self] _, updatedTypes, completionHandler, error in
            let completion = ObserverCompletion(finish: completionHandler)
            guard let self else {
                completion.finish()
                return
            }
            // Sendable: HKSampleType is Sendable; map to identifiers immediately.
            let updated = (updatedTypes ?? Set(typeMap.keys)).compactMap { typeMap[$0] }
            Task {
                if let error {
                    await self.eventLog.log(.error, "Observer error: \(error)")
                    completion.finish()
                    return
                }
                // Deliberately no sync here. HealthKit fires this handler many
                // times in a row for one logical change (up to 93 times in five
                // seconds in production), and doing the work inline made every
                // one of those a separate wake with its own queries and its own
                // tiny upload. Hand it to the coalescer instead.
                await self.enqueueObserverUpdate(types: updated, completion: completion)
            }
        }
        healthStore.execute(query)
        observerQuery = query
        await eventLog.log(.info, "Observer query registered for \(typeMap.count) types")

        for (sampleType, identifier) in typeToIdentifier {
            do {
                // .immediate is a request, not a guarantee — HealthKit silently
                // enforces per-type maximums (e.g. steps is at most hourly).
                try await healthStore.enableBackgroundDelivery(for: sampleType, frequency: .immediate)
            } catch {
                await eventLog.log(.warn, type: identifier, "Background delivery unavailable: \(error) — foreground observation only")
            }
        }
    }

    /// Add one observer delivery to the current burst, arming the flush if this
    /// is the burst's first callback.
    ///
    /// The deadline is set by the *first* callback and deliberately never
    /// extended by later ones: a self-restarting timer would let an unbroken
    /// stream of callbacks postpone the work indefinitely. Anything arriving
    /// after the flush simply forms the next (small) burst.
    func enqueueObserverUpdate(types: [String], completion: ObserverCompletion) {
        pendingObserverTypes.formUnion(types)
        pendingObserverCompletions.append(completion)

        guard observerCoalesceWindow > 0 else {
            Task { await self.flushObserverUpdates() }
            return
        }
        guard observerFlushTask == nil else { return }
        let window = observerCoalesceWindow
        observerFlushTask = Task { [weak self] in
            try? await Task.sleep(for: .seconds(window))
            // stopObserving() cancels this task and releases the pending
            // completions itself; a cancelled flush must not run, or it would
            // nil out a newer burst's flush task and flush that burst early.
            guard !Task.isCancelled else { return }
            await self?.flushObserverUpdates()
        }
    }

    /// Run everything the burst reported as one wake, then release every
    /// completion handler it collected.
    func flushObserverUpdates() async {
        observerFlushTask = nil
        let types = pendingObserverTypes
        let completions = pendingObserverCompletions
        pendingObserverTypes = []
        pendingObserverCompletions = []
        // HealthKit stops waking the app entirely after three unacknowledged
        // deliveries, so every exit path has to release these — exactly once.
        let acknowledged = BackgroundTaskCompletionGate()
        let acknowledge: @Sendable () -> Void = {
            _ = acknowledged.claim { for completion in completions { completion.finish() } }
        }
        defer { acknowledge() }
        guard !types.isEmpty else { return }
        let deliveries = completions.count
        // A delivery buys the app seconds at most; ask for the background time
        // iOS grants on request, and be cancelled — not frozen — when it ends.
        // On expiry HealthKit is acknowledged from the handler itself: the
        // cancelled wake may not unwind to the `defer` before iOS suspends it.
        _ = await backgroundExecution("PulsHealth observer wake", acknowledge) {
            await self.runObserverWake(types: types, deliveries: deliveries)
        }
    }

    /// How long an observer wake stays for types another run holds.
    static let observerWaitLimit: Duration = .seconds(25)

    /// Set when the app has started a backfill that claims its types from a
    /// task of its own — the iOS 26 continued-processing task — and cleared the
    /// moment a backfill claims. Until then observer wakes leave types still
    /// backfilling alone (`expectBackfill`).
    private var backfillExpectedUntil: ContinuousClock.Instant?

    /// Tell the engine a backfill is on its way. Registering the observer
    /// query triggers a wake two seconds later, and a continued-processing
    /// task can take longer than that to start and claim its types; that wake
    /// used to take them first and run the whole history one upload at a time.
    /// Bounded, so a backfill that never comes defers those types by a minute
    /// at most.
    public func expectBackfill(within limit: Duration = .seconds(60)) {
        backfillExpectedUntil = .now + limit
    }

    /// Suspend until none of `keys` is claimed, `limit` has passed, or the task
    /// is cancelled — whichever comes first.
    func waitForRelease(of keys: Set<String>, upTo limit: Duration) async {
        let deadline = ContinuousClock.now + limit
        while !activeSyncs.isDisjoint(with: keys), ContinuousClock.now < deadline {
            do { try await Task.sleep(for: .milliseconds(250)) } catch { return }
        }
    }

    private func runObserverWake(types: Set<String>, deliveries: Int) async {
        guard let configurationUse = beginConfigurationUse() else { return }
        defer { activeSyncs.remove(configurationUse) }
        let sorted = types.sorted()

        // A locked device means HealthKit is unreadable: each type would fail
        // with errorDatabaseInaccessible and log a warning, which is where ~260
        // wasted queries a day were going. Record the wake so the skip stays
        // visible in Background Activity, but do no work — the anchors are
        // untouched, so the next unlocked wake collects exactly this data.
        guard await isHealthDataAccessible() else {
            let wake = await beginWake(
                .observer, detail: "\(sorted.count) type(s) — device locked, skipped")
            await finishWake(wake, outcome: .skippedLocked)
            return
        }

        var detail = "\(sorted.count) type(s): \(sorted.joined(separator: ", "))"
        if deliveries > 1 { detail += " — coalesced from \(deliveries) deliveries" }
        let wake = await beginWake(.observer, detail: detail)
        // Ahead of the merged pass's claims, as in `syncAllEnabled`. An
        // observer wake can be the first sync of a newly enabled type, and
        // the date it reads under has to be on record for a later widening
        // to be noticed.
        await refreshReadableHistory()
        let rawEnabled = await store.configuration.enabledTypes
        var rawTypes = sorted.filter { rawEnabled.contains($0) }
        if let until = backfillExpectedUntil, ContinuousClock.now < until {
            // A backfill the app has started is about to claim these; leave the
            // ones still backfilling to it rather than race it for them.
            var settled: [String] = []
            for id in rawTypes where await store.state(for: id).backfillComplete {
                settled.append(id)
            }
            rawTypes = settled
        }
        await WakeScope.$current.withValue(wake) {
            // One merged pass over the raw types, so a burst touching many types
            // produces a couple of full uploads rather than one tiny upload each.
            await syncTypesMerged(rawTypes, reason: .incremental)
            // Types another run holds — a backfill, or one an earlier wake left
            // behind — were only marked for that run to repeat. Stay for it,
            // within the time this wake holds, rather than acknowledging
            // HealthKit at once and letting iOS suspend the app mid-page.
            await waitForRelease(of: Set(rawTypes), upTo: Self.observerWaitLimit)
            // Raw workout pages defer routes and intra-workout streams (`deferEnrichment: true`
            // in `MergedSync.queryPage`), so enrich workouts here whenever workouts (or
            // `HKSeriesType.workoutRoute()`) updated or a recent workout still needs its
            // late-arriving Watch route/series enriched.
            await refreshWorkoutEnrichmentIfNeeded(updatedRawTypes: Set(rawTypes))
            // Aggregate-only types are observed too and must not get a
            // raw-sample sync, only their aggregate recomputes.
            await withTaskGroup(of: Void.self) { group in
                for identifier in sorted {
                    group.addTask {
                        await self.syncAggregates(forType: identifier, reason: .incremental)
                    }
                }
            }
            await refreshActivitySummaryIfStale()
        }
        // Past the debounce: iOS may suspend the app the moment this returns.
        await store.persistNow()
        await finishWake(wake, outcome: Task.isCancelled ? .expired : .completed)
    }

    public func stopObserving() {
        observerFlushTask?.cancel()
        observerFlushTask = nil
        // Releasing these is not optional: HealthKit counts an unacknowledged
        // delivery against the app whether or not the query is still running.
        for completion in pendingObserverCompletions { completion.finish() }
        pendingObserverCompletions = []
        pendingObserverTypes = []
        if let query = observerQuery {
            healthStore.stop(query)
            observerQuery = nil
        }
    }

    public func disableBackgroundDelivery() async {
        try? await healthStore.disableAllBackgroundDelivery()
    }

    // MARK: - Cooldowns and unreadable anchors

    /// Whether a run ignores type cooldowns (`TypeSyncState.cooldownUntil`):
    /// one a person asked for — Sync Now, pull-to-refresh, Start Backfill, a
    /// single type's Sync — rather than an observer wake, a background task
    /// or a foreground pass. Its upload either goes through, which lifts the
    /// cooldown, or is refused again, which lengthens it.
    nonisolated static func bypassesCooldown(_ reason: SyncReason) -> Bool {
        reason == .manual || WakeScope.current?.trigger == .manual
    }

    /// The anchor stored for `identifier` on `pass`'s stream, decoded. One
    /// that will not decode (a restored or downgraded state file) is not
    /// inspected and not retried forever: the stream is started over
    /// (`SyncStateStore.restartForUnreadableAnchor`) and nil returned, so
    /// this run reads it from the start. Re-sending what the server has is
    /// a no-op. The caller must hold the type's claim.
    func storedAnchor(_ data: Data?, of identifier: String, pass: SweepPass) async -> HKQueryAnchor? {
        do {
            return try decodeAnchor(data)
        } catch {
            await store.restartForUnreadableAnchor(identifier, recentStreamOnly: pass == .recent)
            await eventLog.log(
                .warn, type: identifier,
                pass == .recent
                    ? "Stored recent-window anchor unreadable (\(error)); re-reading the recent window"
                    : "Stored anchor unreadable (\(error)); starting this type's history over — the server ignores samples it already has")
            return nil
        }
    }

    // MARK: - Anchor codec

    nonisolated func decodeAnchor(_ data: Data?) throws -> HKQueryAnchor? {
        guard let data else { return nil }
        return try NSKeyedUnarchiver.unarchivedObject(ofClass: HKQueryAnchor.self, from: data)
    }

    nonisolated func encodeAnchor(_ anchor: HKQueryAnchor?) throws -> Data? {
        guard let anchor else { return nil }
        return try NSKeyedArchiver.archivedData(withRootObject: anchor, requiringSecureCoding: true)
    }
}

/// How a pass runner tells its failures apart. The per-type and merged
/// sweeps, the aggregate, ring and workout-enrichment passes each used to
/// spell out the same four `catch` clauses; this is that classification once,
/// and each runner still decides what to log and record for each case.
enum PassFailure {
    /// Health access for the type was never asked for. Recorded as
    /// `SyncError.authorizationNotDetermined`.
    case authorizationNotDetermined
    /// The device is locked and HealthKit unreadable. Not a failure: progress
    /// is untouched and the next unlocked run catches up.
    case databaseLocked
    /// Background time ran out. Not a failure either.
    case cancelled
    /// Anything else, recorded as the pass's error.
    case failed(any Error)

    init(_ error: any Error) {
        let unwrapped: any Error = (error as? WorkoutRouteReadError)?.underlyingError ?? error
        if let healthKitError = unwrapped as? HKError,
           healthKitError.code == .errorAuthorizationNotDetermined {
            self = .authorizationNotDetermined
        } else if let healthKitError = unwrapped as? HKError,
                  healthKitError.code == .errorDatabaseInaccessible {
            self = .databaseLocked
        } else if unwrapped is CancellationError {
            self = .cancelled
        } else {
            self = .failed(error)
        }
    }
}

public enum SyncError: Error, LocalizedError {
    case accessRestricted
    case configurationBusy
    case typeBusy(String)
    case healthDataUnavailable
    case authorizationNotDetermined
    case unknownType(String)
    case reconciliationUnsupported(String)
    /// iOS 27: HealthKit could not say how much of the type's history the
    /// app may read, and a pass that overwrites or deletes server data to
    /// match what it reads — an aggregate series, the rings, reconciliation
    /// — will not guess.
    case readableHistoryUnknown(String)
    /// Reconciliation found nothing of the type on the device in any month
    /// the server has rows for, and nothing confirmed that the app may read
    /// it. Read access that is off answers every query with exactly that,
    /// so nothing was deleted.
    case reconciliationUnreadable(String, serverRows: Int64)

    public var errorDescription: String? {
        switch self {
        case .accessRestricted:
            return "Choose up to 5 health data types or upgrade to PulsHealth Pro to sync this selection."
        case .configurationBusy:
            return "Database settings are being applied. Try again in a moment."
        case .typeBusy(let identifier):
            return "A sync or reconciliation is already running for \(identifier). Try again when it finishes."
        case .healthDataUnavailable:
            return "HealthKit is not available on this device"
        case .authorizationNotDetermined:
            return "Health access not granted — tap Grant Health Access on the Explore tab"
        case .unknownType(let identifier):
            return "Unknown type identifier: \(identifier)"
        case .reconciliationUnsupported(let identifier):
            return "Reconciliation only covers quantity, category, and workout types (\(identifier))"
        case .readableHistoryUnknown(let name):
            return "Could not tell how much \(name) history iOS lets PulsHealth read, so nothing that could overwrite or delete server data was sent for it."
        case .reconciliationUnreadable(let name, let serverRows):
            return "Health returned no \(name) samples, but the database has \(serverRows). A type whose Health access is off reads the same as one with no data, so nothing was deleted. Check Settings → Privacy & Security → Health → PulsHealth."
        }
    }
}
