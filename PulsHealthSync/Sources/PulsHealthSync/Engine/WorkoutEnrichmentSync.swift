import CryptoKit
import Foundation
import HealthKit

// The last two phases of `syncAllEnabled`: GPS routes, then intra-workout
// streams, run *after* all basic data is synced.
//
// Two failures motivated this design, both seen on real devices:
//
//   1. CONTENTION. Attaching routes + series inline to every workout batch starved
//      the rest of the sync: a workout batch couldn't upload until many per-workout
//      HealthKit queries finished, so the server idled waiting on the phone
//      (parse_ms ballooned, throughput collapsed). Fix: move enrichment to its own
//      late phases so basic data finishes first.
//
//   2. OVERSIZED BATCHES → INFINITE RETRY. Uploading an entire page of workouts'
//      streams as one atomic batch produced batches too big to upload+insert inside
//      the client request timeout. The phone cancelled mid-insert ("context
//      canceled"), the server transaction rolled back (nothing committed), the
//      watermark never advanced, and the resync re-sent the SAME oversized batch —
//      forever. Fix (this file): chunk enrichment by a *point budget*, split a
//      single workout's points across as many batches as needed, upload
//      sequentially, and advance the watermark per fully-acked workout.
//
// No wire/server changes are needed for either fix. Routes and series already ride
// their own NDJSON lines (`{"route":...}` / `{"series":...}`) carrying the workout
// UUID, and the server inserts them by joining on that UUID with
// `ON CONFLICT DO NOTHING`. So a workout row can land in the raw sweep, and its
// routes/series can arrive split across many later batches — fully idempotent
// end-to-end.
//
// Like activity summaries (see ActivitySummarySync) these have no HKQueryAnchor:
// each phase enumerates workouts ascending by `endDate`, uploads point-capped
// route-/series-only batches, and advances a singleton `computedThrough` watermark
// to each workout's `endDate` only after *all* of that workout's chunks are acked
// (anchor-after-ack) and no earlier route-capable workout is still pending late
// Apple Watch transfer or retry.

/// A workout-enrichment payload (route or series) whose points can be split so a
/// single workout's data never exceeds one batch's point budget.
protocol EnrichmentPayload {
    var pointCount: Int { get }
    /// A copy of this payload carrying only `points[range]` (same workout UUID /
    /// type / unit), so splitting stays idempotent server-side.
    func slice(_ range: Range<Int>) -> Self
}

extension RoutePayload: EnrichmentPayload {
    var pointCount: Int { points.count }
    func slice(_ range: Range<Int>) -> RoutePayload {
        RoutePayload(workoutUUID: workoutUUID, points: Array(points[range]))
    }
}

extension WorkoutSeriesPayload: EnrichmentPayload {
    var pointCount: Int { points.count }
    func slice(_ range: Range<Int>) -> WorkoutSeriesPayload {
        WorkoutSeriesPayload(workoutUUID: workoutUUID, type: type, unit: unit, points: Array(points[range]))
    }
}

/// Lightweight workout descriptor for route enrichment and testing without
/// requiring synthesized `HKWorkoutRoute` objects inside unit tests.
public struct WorkoutRouteCandidate: Sendable, Equatable {
    public var uuid: UUID
    public var activityType: HKWorkoutActivityType
    public var startDate: Date
    public var endDate: Date
    /// Explicit value of `HKMetadataKeyIndoorWorkout` when present on the workout.
    /// Nil when HealthKit metadata omitted the key.
    public var isExplicitlyIndoor: Bool?

    public init(
        uuid: UUID,
        activityType: HKWorkoutActivityType,
        startDate: Date,
        endDate: Date,
        isExplicitlyIndoor: Bool? = nil
    ) {
        self.uuid = uuid
        self.activityType = activityType
        self.startDate = startDate
        self.endDate = endDate
        self.isExplicitlyIndoor = isExplicitlyIndoor
    }

    public init(workout: HKWorkout) {
        self.uuid = workout.uuid
        self.activityType = workout.workoutActivityType
        self.startDate = workout.startDate
        self.endDate = workout.endDate
        self.isExplicitlyIndoor = Self.extractIndoorMetadata(from: workout.metadata)
    }

    public static func extractIndoorMetadata(from metadata: [String: Any]?) -> Bool? {
        guard let raw = metadata?[HKMetadataKeyIndoorWorkout] else { return nil }
        if let b = raw as? Bool { return b }
        if let n = raw as? NSNumber { return n.boolValue }
        return nil
    }
}

/// Abstraction over HealthKit workout & route reading so the route state machine,
/// late-arrival retries, error classification, and idempotent uploads can be
/// deterministically tested.
public protocol WorkoutRouteDataSource: Sendable {
    func fetchWorkoutCandidates(endedAfter: Date) async throws -> [WorkoutRouteCandidate]
    func routePayloads(for candidate: WorkoutRouteCandidate) async throws -> [RoutePayload]
}

/// Production `WorkoutRouteDataSource` backed by `HKHealthStore` and `SeriesEnricher`.
actor HealthKitWorkoutRouteDataSource: WorkoutRouteDataSource {
    private let enricher: SeriesEnricher
    private let fetchWorkoutsBlock: @Sendable (Date) async throws -> [HKWorkout]
    private var workoutsByUUID: [UUID: HKWorkout] = [:]

    init(
        healthStore: HKHealthStore,
        fetchWorkouts: @escaping @Sendable (Date) async throws -> [HKWorkout]
    ) {
        self.enricher = SeriesEnricher(healthStore: healthStore)
        self.fetchWorkoutsBlock = fetchWorkouts
    }

    func fetchWorkoutCandidates(endedAfter: Date) async throws -> [WorkoutRouteCandidate] {
        let workouts = try await fetchWorkoutsBlock(endedAfter)
        workoutsByUUID = Dictionary(uniqueKeysWithValues: workouts.map { ($0.uuid, $0) })
        return workouts.map(WorkoutRouteCandidate.init(workout:))
    }

    func routePayloads(for candidate: WorkoutRouteCandidate) async throws -> [RoutePayload] {
        guard let workout = workoutsByUUID[candidate.uuid] else { return [] }
        return try await enricher.routePayloads(for: workout)
    }
}

/// Thrown when the ingest server returns a receipt whose `routePoints` count does
/// not acknowledge the uploaded route points.
public enum WorkoutRouteUploadError: Error, LocalizedError, Sendable, Equatable {
    case receiptMismatch(workoutUUID: UUID, expected: Int, acknowledged: Int)

    public var errorDescription: String? {
        switch self {
        case .receiptMismatch(let workoutUUID, let expected, let acknowledged):
            return "Server receipt acknowledged \(acknowledged) of \(expected) route points for workout \(workoutUUID)"
        }
    }
}

/// Audit / reconciliation summary for recent workout routes.
public struct WorkoutRouteAuditReport: Sendable, Equatable {
    public var checkedCount: Int = 0
    public var verified: [UUID] = []
    public var pendingLateArrival: [UUID] = []
    public var noRouteInHealthKit: [UUID] = []
    public var readFailed: [UUID] = []
    public var uploadFailed: [UUID] = []
    public var indoorSkipped: [UUID] = []
    public var nonRouteSkipped: [UUID] = []
    public var records: [UUID: WorkoutRouteSyncRecord] = [:]

    public init() {}

    /// Route-capable workouts that still lack a verified route and require retry or attention.
    public var unresolvedRouteCapableUUIDs: [UUID] {
        pendingLateArrival + readFailed + uploadFailed
    }

    public var summary: String {
        "\(verified.count) verified, \(pendingLateArrival.count) pending Watch transfer, \(noRouteInHealthKit.count) without route in HealthKit, \(readFailed.count) read failed, \(uploadFailed.count) upload failed, \(indoorSkipped.count) indoor"
    }
}

enum WorkoutEnrichmentSchedule {
    /// Pick the lower bound for one enrichment run. Any initial, monthly, or
    /// forced full pass resumes from its own persisted per-workout cursor.
    /// When `earliestPendingWorkoutEnd` is present (e.g. a workout whose route is
    /// still `.pendingLateArrival`, `.readFailed`, or `.uploadFailed`), the lower
    /// bound is clamped so that workout is never stranded behind the watermark.
    static func window(
        startDate: Date,
        computedThrough: Date?,
        lastFullRecomputeAt: Date?,
        fullRecomputeStartedAt: Date?,
        fullRecomputeThrough: Date?,
        earliestPendingWorkoutEnd: Date? = nil,
        now: Date
    ) -> (from: Date, fullPass: Bool) {
        let fullPass = fullRecomputeStartedAt != nil || computedThrough == nil
            || lastFullRecomputeAt.map {
                now.timeIntervalSince($0) > AggregateSchedule.fullRecomputeInterval
            } ?? true
        let baseFrom: Date
        if fullPass {
            if let fullRecomputeThrough {
                // Re-cover one second so workouts sharing the cursor's end time
                // cannot be skipped by boundary semantics. Uploads are idempotent.
                baseFrom = max(startDate, fullRecomputeThrough.addingTimeInterval(-1))
            } else {
                baseFrom = startDate
            }
        } else {
            let lookback = AggregateSchedule.lookback(intervalSeconds: 86_400)
            let cursor = computedThrough ?? startDate
            baseFrom = max(startDate, cursor.addingTimeInterval(-lookback))
        }
        if let pendingEnd = earliestPendingWorkoutEnd {
            let clamped = max(startDate, min(baseFrom, pendingEnd.addingTimeInterval(-1)))
            return (clamped, fullPass)
        }
        return (baseFrom, fullPass)
    }
}

extension HealthSyncEngine {
    /// How long after `workout.endDate` a route-capable workout with 0 route points
    /// in HealthKit remains in `.pendingLateArrival` while Apple Watch may still be
    /// transferring the `HKWorkoutRoute` series to the iPhone.
    public static let lateRouteGraceInterval: TimeInterval = 24 * 3_600

    /// Pack point-bearing payloads into batches whose total point count never
    /// exceeds `budget`, splitting an individual payload (and thus a single
    /// pathological workout) across batches when it alone exceeds the budget.
    /// Shared by both enrichment phases. Empty payloads are dropped.
    static func packByPointBudget<T: EnrichmentPayload>(_ payloads: [T], budget: Int) -> [[T]] {
        let cap = max(1, budget)
        var batches: [[T]] = []
        var current: [T] = []
        var currentCount = 0
        for payload in payloads {
            let total = payload.pointCount
            guard total > 0 else { continue }
            var offset = 0
            while offset < total {
                if currentCount >= cap {
                    batches.append(current)
                    current = []
                    currentCount = 0
                }
                let take = min(total - offset, cap - currentCount)
                current.append(payload.slice(offset ..< offset + take))
                currentCount += take
                offset += take
            }
        }
        if !current.isEmpty { batches.append(current) }
        return batches
    }

    /// Deterministic `batchID` for a route enrichment chunk so retrying an un-acked
    /// route upload deduplicates cleanly at the batch level while `reuploadEpoch`
    /// allows explicit reconciliation re-uploads to execute `insertRoutePoints`
    /// (`ON CONFLICT (workout_uuid, ts) DO NOTHING`) on the server.
    public static func deterministicRouteBatchID(
        deviceID: String,
        workoutUUID: UUID,
        chunkIndex: Int,
        pointCount: Int,
        firstTimestamp: Date?,
        lastTimestamp: Date?,
        reuploadEpoch: Int = 0
    ) -> UUID {
        let firstMS = firstTimestamp.map { Int64(($0.timeIntervalSince1970 * 1000).rounded()) } ?? 0
        let lastMS = lastTimestamp.map { Int64(($0.timeIntervalSince1970 * 1000).rounded()) } ?? 0
        let parts = [
            deviceID,
            workoutUUID.uuidString.lowercased(),
            String(chunkIndex),
            String(pointCount),
            String(firstMS),
            String(lastMS),
            String(reuploadEpoch),
        ]
        let digest = Array(SHA256.hash(data: Data(parts.joined(separator: "|").utf8)))
        return UUID(uuid: (
            digest[0], digest[1], digest[2], digest[3],
            digest[4], digest[5], digest[6], digest[7],
            digest[8], digest[9], digest[10], digest[11],
            digest[12], digest[13], digest[14], digest[15]
        ))
    }

    /// `HKWorkoutActivityType`s that record a GPS route — outdoor, location-based.
    /// Used only as the "definitely query" half of `mayHaveWorkoutRoute`; missing
    /// one is harmless (it falls through to the conservative default).
    static let routeCapableActivityTypes: Set<HKWorkoutActivityType> = [
        .walking, .running, .cycling, .hiking, .swimming,
        .wheelchairWalkPace, .wheelchairRunPace,
        .crossCountrySkiing, .downhillSkiing, .snowboarding, .skatingSports,
        .paddleSports, .rowing, .sailing, .surfingSports, .golf, .hunting, .fishing,
    ]

    /// `HKWorkoutActivityType`s that are stationary / indoor and never record a GPS
    /// route — the only types we skip the route query for. Kept deliberately small
    /// and unambiguous; ambiguous types (could be indoor or outdoor) are left off so
    /// they're still queried.
    static let nonRouteActivityTypes: Set<HKWorkoutActivityType> = [
        .traditionalStrengthTraining, .functionalStrengthTraining, .coreTraining,
        .yoga, .pilates, .barre, .flexibility, .cooldown, .mindAndBody,
        .preparationAndRecovery, .stairClimbing, .elliptical, .jumpRope, .stepTraining,
    ]

    /// Whether to bother running the route query for a workout activity type.
    /// Conservative: only the explicit `nonRouteActivityTypes` are skipped;
    /// route-capable and every other (unknown / ambiguous) type is queried, so a
    /// real route is never dropped.
    static func mayHaveWorkoutRoute(_ type: HKWorkoutActivityType) -> Bool {
        if routeCapableActivityTypes.contains(type) { return true }
        if nonRouteActivityTypes.contains(type) { return false }
        return true // unsure → query rather than risk dropping a route
    }

    /// Whether a specific workout candidate can have a GPS route. Explicitly indoor
    /// workouts (`HKIndoorWorkout == true`) and `nonRouteActivityTypes` return false;
    /// missing routes are never inferred to mean indoor.
    static func mayHaveWorkoutRoute(_ candidate: WorkoutRouteCandidate) -> Bool {
        guard mayHaveWorkoutRoute(candidate.activityType) else { return false }
        if candidate.isExplicitlyIndoor == true { return false }
        return true
    }

    /// Routes phase: fetch and upload GPS routes for workouts. Overlap-guarded.
    public func syncWorkoutRoutes(reason: SyncReason = .incremental) async {
        await runGuardedWorkoutEnrichment(.routes, reason: reason)
    }

    /// Streams phase: fetch and upload the intra-workout streams (HR/power/cadence/…).
    /// Overlap-guarded.
    public func syncWorkoutStreams(reason: SyncReason = .incremental) async {
        await runGuardedWorkoutEnrichment(.streams, reason: reason)
    }

    /// Testable entry point that runs the route enrichment state machine against a
    /// custom `WorkoutRouteDataSource` (used by unit tests and `auditWorkoutRoutes`).
    public func syncWorkoutRoutes(
        using dataSource: any WorkoutRouteDataSource,
        reason: SyncReason = .incremental,
        now: Date = Date(),
        forceReupload: Bool = false
    ) async {
        await ensureTransport()
        let typeID = HealthTypeCatalog.workoutIdentifier
        guard let transport else {
            await eventLog.log(.error, type: typeID, "No transport configured — set server URL and token")
            return
        }
        let config = await store.configuration
        guard config.includeWorkoutRoutes else { return }
        let calendar = Calendar.current
        let startDay = calendar.startOfDay(for: config.startDate)
        let budget = config.maxEnrichmentPointsPerBatch
        let state = await store.workoutEnrichmentState(.routes)
        let schedule = WorkoutEnrichmentSchedule.window(
            startDate: startDay,
            computedThrough: state.computedThrough,
            lastFullRecomputeAt: state.lastFullRecomputeAt,
            fullRecomputeStartedAt: state.fullRecomputeStartedAt,
            fullRecomputeThrough: state.fullRecomputeThrough,
            earliestPendingWorkoutEnd: state.earliestPendingRouteWorkoutEnd,
            now: now
        )
        let from = await limitedSyncStart(schedule.from)
        guard from <= now else { return }
        await runWorkoutRouteEnrichmentPass(
            from: from,
            fullPass: schedule.fullPass,
            budget: budget,
            reason: reason,
            now: now,
            forceReupload: forceReupload,
            dataSource: dataSource,
            transport: transport
        )
    }

    /// Audit recent workouts for route completeness and return a categorized
    /// reconciliation report (`verified`, `pendingLateArrival`, `noRouteInHealthKit`,
    /// `readFailed`, `uploadFailed`, `indoorSkipped`, `nonRouteSkipped`).
    /// When `repairMissing` is true, runs a route sync pass first so any newly
    /// arrived or retry-eligible routes are uploaded and verified before reporting.
    public func auditWorkoutRoutes(
        lookback: TimeInterval = 7 * 86_400,
        using customDataSource: (any WorkoutRouteDataSource)? = nil,
        now: Date = Date(),
        repairMissing: Bool = true
    ) async -> WorkoutRouteAuditReport {
        let dataSource: any WorkoutRouteDataSource
        if let customDataSource {
            dataSource = customDataSource
        } else {
            let hs = healthStore
            dataSource = HealthKitWorkoutRouteDataSource(healthStore: hs) { endedAfter in
                try await self.fetchWorkouts(endedAfter: endedAfter)
            }
        }
        if repairMissing {
            await syncWorkoutRoutes(using: dataSource, reason: .reconciliation, now: now)
        }
        let from = now.addingTimeInterval(-max(0, lookback))
        let candidates = (try? await dataSource.fetchWorkoutCandidates(endedAfter: from)) ?? []
        let storedRecords = await store.allWorkoutRouteRecords()
        var report = WorkoutRouteAuditReport()
        report.checkedCount = candidates.count
        for candidate in candidates {
            if !Self.mayHaveWorkoutRoute(candidate.activityType) {
                report.nonRouteSkipped.append(candidate.uuid)
                continue
            }
            if candidate.isExplicitlyIndoor == true {
                report.indoorSkipped.append(candidate.uuid)
                if let rec = storedRecords[candidate.uuid] {
                    report.records[candidate.uuid] = rec
                }
                continue
            }
            if let rec = storedRecords[candidate.uuid] {
                report.records[candidate.uuid] = rec
                switch rec.status {
                case .verified:
                    report.verified.append(candidate.uuid)
                case .pendingLateArrival:
                    report.pendingLateArrival.append(candidate.uuid)
                case .noRouteInHealthKit:
                    report.noRouteInHealthKit.append(candidate.uuid)
                case .readFailed:
                    report.readFailed.append(candidate.uuid)
                case .uploadFailed:
                    report.uploadFailed.append(candidate.uuid)
                case .indoorWorkout:
                    report.indoorSkipped.append(candidate.uuid)
                }
            } else {
                // Never evaluated yet -> treat as pending if within grace window, else missing.
                if now.timeIntervalSince(candidate.endDate) < Self.lateRouteGraceInterval {
                    report.pendingLateArrival.append(candidate.uuid)
                } else {
                    report.noRouteInHealthKit.append(candidate.uuid)
                }
            }
        }
        return report
    }

    /// Whether an observer wake should run a workout-enrichment phase (`.routes`
    /// or `.streams`).
    ///
    /// `MergedSync.queryPage` uploads raw workout rows with `deferEnrichment: true`,
    /// leaving routes and streams to `syncWorkoutRoutes` / `syncWorkoutStreams`.
    /// Without running enrichment on observer wakes, workouts synced in the
    /// background land on the server with 0 route points until the user opens the
    /// app in the foreground (`syncAllEnabled`).
    ///
    /// Runs when:
    /// 1. `updatedRawTypes` includes `HKWorkoutTypeIdentifier` (triggered by a new
    ///    `HKWorkout` or `HKSeriesType.workoutRoute()` delivery),
    /// 2. `enrichmentState` has any workout route in `.pendingLateArrival`,
    ///    `.readFailed`, or `.uploadFailed` whose retry backoff is due at `now`,
    /// 3. A workout sync has completed since enrichment last finished (e.g. a prior
    ///    wake uploaded the workout row and expired before enrichment completed), or
    /// 4. A workout synced within `lateArrivalWindow` (default 6 h) and at least
    ///    `minimumInterval` (default 30 min) has passed since enrichment last ran,
    ///    so late Watch-to-iPhone GPS/HR transfers self-heal on subsequent wakes
    ///    without re-uploading the 7-day lookback window on every wake indefinitely.
    static func shouldRefreshWorkoutEnrichment(
        updatedRawTypes: Set<String>,
        workoutState: TypeSyncState,
        enrichmentState: WorkoutEnrichmentState,
        now: Date = Date(),
        lateArrivalWindow: TimeInterval = 6 * 3600,
        minimumInterval: TimeInterval = 1800
    ) -> Bool {
        if updatedRawTypes.contains(HealthTypeCatalog.workoutIdentifier) {
            return true
        }
        if enrichmentState.hasDueRouteRetries(at: now) {
            return true
        }
        guard workoutState.backfillComplete, let lastWorkoutSync = workoutState.lastSyncAt else {
            return false
        }
        guard let lastComputed = enrichmentState.lastComputedAt else {
            return true
        }
        if lastWorkoutSync > lastComputed {
            return true
        }
        if now.timeIntervalSince(lastWorkoutSync) <= lateArrivalWindow,
           now.timeIntervalSince(lastComputed) >= minimumInterval {
            return true
        }
        return false
    }

    /// Run workout route and stream enrichment during an observer wake when needed.
    func refreshWorkoutEnrichmentIfNeeded(
        updatedRawTypes: Set<String>,
        now: Date = Date(),
        lateArrivalWindow: TimeInterval = 6 * 3600,
        minimumInterval: TimeInterval = 1800
    ) async {
        let config = await store.configuration
        guard config.enabledTypes.contains(HealthTypeCatalog.workoutIdentifier),
              !Task.isCancelled else { return }
        let workoutState = await store.state(for: HealthTypeCatalog.workoutIdentifier)
        if config.includeWorkoutRoutes {
            let routesState = await store.workoutEnrichmentState(.routes)
            if Self.shouldRefreshWorkoutEnrichment(
                updatedRawTypes: updatedRawTypes,
                workoutState: workoutState,
                enrichmentState: routesState,
                now: now,
                lateArrivalWindow: lateArrivalWindow,
                minimumInterval: minimumInterval
            ) {
                await syncWorkoutRoutes(reason: .incremental)
            }
        }
        guard !Task.isCancelled else { return }
        if config.includeWorkoutEnhancedData {
            let streamsState = await store.workoutEnrichmentState(.streams)
            if Self.shouldRefreshWorkoutEnrichment(
                updatedRawTypes: updatedRawTypes,
                workoutState: workoutState,
                enrichmentState: streamsState,
                now: now,
                lateArrivalWindow: lateArrivalWindow,
                minimumInterval: minimumInterval
            ) {
                await syncWorkoutStreams(reason: .incremental)
            }
        }
    }

    func runGuardedWorkoutEnrichment(
        _ kind: WorkoutEnrichmentKind,
        reason: SyncReason,
        queueResyncWhenActive: Bool = true
    ) async {
        guard !configurationMutationInProgress else { return }
        let key = "workout-enrich:\(kind.rawValue)"
        guard !activeSyncs.contains(key) else {
            // Ordinary observer/manual requests coalesce in memory. Forced
            // requests are already durable and the active loop checks the store
            // before exiting, so adding a second wake-up is unnecessary.
            if queueResyncWhenActive { pendingResync.insert(key) }
            return
        }
        activeSyncs.insert(key)
        defer { activeSyncs.remove(key) }

        var nextReason = reason
        while true {
            // This one store operation clears pending intent and resets the
            // durable full-pass cursor. A crash on either side is recoverable.
            let forcedRestart = await store.consumeForcedWorkoutEnrichmentFullRecompute(kind)
            await runWorkoutEnrichmentSync(
                kind, reason: forcedRestart ? .reconciliation : nextReason)
            nextReason = .incremental
            let pending = pendingResync.remove(key) != nil
            let forcedPending = await store.hasForcedWorkoutEnrichmentFullRecompute(kind)
            if !pending && !forcedPending { break }
        }
    }

    private func runWorkoutEnrichmentSync(
        _ kind: WorkoutEnrichmentKind, reason: SyncReason
    ) async {
        await ensureTransport()
        let typeID = HealthTypeCatalog.workoutIdentifier
        guard let transport else {
            await eventLog.log(.error, type: typeID, "No transport configured — set server URL and token")
            return
        }

        guard await allowsSync(types: [typeID]) else { return }
        let config = await store.configuration
        // Self-gate on the relevant config flag; a disabled phase is a no-op.
        switch kind {
        case .routes: guard config.includeWorkoutRoutes else { return }
        case .streams: guard config.includeWorkoutEnhancedData else { return }
        }

        let calendar = Calendar.current
        let startDay = calendar.startOfDay(for: config.startDate)
        let now = Date()
        let budget = config.maxEnrichmentPointsPerBatch
        var state = await store.workoutEnrichmentState(kind)
        // iOS 27 limited history access: nothing to clamp here — this phase
        // only follows workouts HealthKit returns, and routes and streams
        // insert without overwriting — but the workouts older than the
        // earliest readable date were invisible to it, and its watermark is
        // past them. A widened grant therefore starts the phase over from
        // the start date (`ReadableHistory`).
        let readable = await readableLimit(for: typeID, recorded: state.readableSince)
        let since = ReadableHistory.effectiveLimit(readable.since, readingFrom: startDay)
        if readable.isFresh {
            switch ReadableHistory.change(from: state.readableSince, to: since) {
            case .widened:
                await store.resetWorkoutEnrichment(kind)
                await eventLog.log(
                    .info, type: typeID,
                    "Workout \(kind.rawValue): Health access widened — re-reading every workout from the start date")
                fallthrough
            case .narrowed:
                await store.updateWorkoutEnrichment(kind) { $0.readableSince = since }
                state = await store.workoutEnrichmentState(kind)
            case .unchanged:
                break
            }
        }
        let fullPassDue = state.computedThrough == nil
            || state.lastFullRecomputeAt.map {
                now.timeIntervalSince($0) > AggregateSchedule.fullRecomputeInterval
            } ?? true
        let fullPass = state.fullRecomputeStartedAt != nil || fullPassDue
        if fullPass, state.fullRecomputeStartedAt == nil {
            let legacyCursor = FullRecomputeMigration.legacyInitialCursor(
                computedThrough: state.computedThrough,
                lastFullRecomputeAt: state.lastFullRecomputeAt,
                fullRecomputeStartedAt: state.fullRecomputeStartedAt)
            await store.beginWorkoutEnrichmentFullRecompute(
                kind, at: now, resumeThrough: legacyCursor)
            state = await store.workoutEnrichmentState(kind)
        }

        // Window [from, now]: a full pass goes to the start date; otherwise trail
        // the watermark by a lookback so recent workouts that gained late Watch
        // route/HR data self-heal, and clamp to any pending/failed route workout.
        let schedule = WorkoutEnrichmentSchedule.window(
            startDate: startDay,
            computedThrough: state.computedThrough,
            lastFullRecomputeAt: state.lastFullRecomputeAt,
            fullRecomputeStartedAt: state.fullRecomputeStartedAt,
            fullRecomputeThrough: state.fullRecomputeThrough,
            earliestPendingWorkoutEnd: kind == .routes ? state.earliestPendingRouteWorkoutEnd : nil,
            now: now
        )
        let from = await limitedSyncStart(schedule.from)
        guard from <= now else { return }

        switch kind {
        case .routes:
            let hs = healthStore
            let dataSource = HealthKitWorkoutRouteDataSource(healthStore: hs) { endedAfter in
                try await self.fetchWorkouts(endedAfter: endedAfter)
            }
            await runWorkoutRouteEnrichmentPass(
                from: from,
                fullPass: fullPass,
                budget: budget,
                reason: reason,
                now: now,
                forceReupload: reason == .reconciliation,
                dataSource: dataSource,
                transport: transport
            )
        case .streams:
            await runWorkoutStreamEnrichmentPass(
                from: from,
                fullPass: fullPass,
                budget: budget,
                reason: reason,
                now: now,
                transport: transport
            )
        }
    }

    private func runWorkoutRouteEnrichmentPass(
        from: Date,
        fullPass: Bool,
        budget: Int,
        reason: SyncReason,
        now: Date,
        forceReupload: Bool,
        dataSource: any WorkoutRouteDataSource,
        transport: SyncTransport
    ) async {
        let typeID = HealthTypeCatalog.workoutIdentifier
        let deviceID = store.deviceID
        let runStart = ContinuousClock.now
        do {
            let candidates = try await dataSource.fetchWorkoutCandidates(endedAfter: from)
            var totalWorkouts = 0
            var totalBatches = 0
            var watermarkHeld = false

            for candidate in candidates {
                try Task.checkCancellation()
                // Skip non-route activity types without querying HealthKit.
                guard Self.mayHaveWorkoutRoute(candidate.activityType) else {
                    if !watermarkHeld {
                        await store.advanceWorkoutEnrichmentWatermark(.routes, to: candidate.endDate, workouts: 1)
                    }
                    totalWorkouts += 1
                    continue
                }

                let existing = await store.workoutRouteRecord(for: candidate.uuid)
                let firstSeen = existing?.firstSeenAt ?? now

                // Explicitly indoor workouts (`HKIndoorWorkout == true`) never record GPS routes.
                if candidate.isExplicitlyIndoor == true {
                    await store.recordWorkoutRouteStatus(WorkoutRouteSyncRecord(
                        workoutUUID: candidate.uuid,
                        activityTypeRaw: candidate.activityType.rawValue,
                        workoutStart: candidate.startDate,
                        workoutEnd: candidate.endDate,
                        isExplicitlyIndoor: true,
                        status: .indoorWorkout,
                        attemptCount: (existing?.attemptCount ?? 0) + 1,
                        firstSeenAt: firstSeen,
                        lastAttemptAt: now
                    ))
                    if !watermarkHeld {
                        await store.advanceWorkoutEnrichmentWatermark(.routes, to: candidate.endDate, workouts: 1)
                    }
                    totalWorkouts += 1
                    continue
                }

                // For already-verified workouts outside the late-arrival grace window,
                // skip re-querying and re-uploading unless forced (e.g. reconciliation).
                let withinGrace = now.timeIntervalSince(candidate.endDate) < Self.lateRouteGraceInterval
                if let existing, existing.status == .verified, !withinGrace, !forceReupload {
                    if !watermarkHeld {
                        await store.advanceWorkoutEnrichmentWatermark(.routes, to: candidate.endDate, workouts: 0)
                    }
                    totalWorkouts += 1
                    continue
                }

                let payloads: [RoutePayload]
                do {
                    payloads = try await dataSource.routePayloads(for: candidate)
                } catch {
                    if SeriesEnricher.isPhaseAbortingError(error) { throw error }
                    let phase = (error as? WorkoutRouteReadError)?.phase ?? "route_query"
                    let attempts = (existing?.attemptCount ?? 0) + 1
                    let nextRetry = now.addingTimeInterval(WorkoutRouteSyncRecord.retryDelay(forAttempt: attempts))
                    await store.recordWorkoutRouteStatus(WorkoutRouteSyncRecord(
                        workoutUUID: candidate.uuid,
                        activityTypeRaw: candidate.activityType.rawValue,
                        workoutStart: candidate.startDate,
                        workoutEnd: candidate.endDate,
                        isExplicitlyIndoor: candidate.isExplicitlyIndoor,
                        status: .readFailed,
                        attemptCount: attempts,
                        firstSeenAt: firstSeen,
                        lastAttemptAt: now,
                        nextRetryAt: nextRetry,
                        expectedPointCount: existing?.expectedPointCount ?? 0,
                        verifiedPointCount: existing?.verifiedPointCount ?? 0,
                        verifiedAt: existing?.verifiedAt,
                        lastErrorPhase: phase,
                        lastErrorMessage: error.localizedDescription
                    ))
                    await eventLog.log(
                        .warn, type: typeID,
                        "Workout route read failed (\(phase)) for workout \(candidate.uuid) (\(candidate.activityType.rawValue)): \(error) — will retry")
                    watermarkHeld = true
                    totalWorkouts += 1
                    continue
                }

                let totalPoints = payloads.reduce(0) { $0 + $1.points.count }
                if totalPoints == 0 {
                    let attempts = (existing?.attemptCount ?? 0) + 1
                    if withinGrace {
                        // Apple Watch route data may still be arriving. Keep in
                        // pendingLateArrival and do NOT advance watermark past this workout.
                        let nextRetry = now.addingTimeInterval(WorkoutRouteSyncRecord.retryDelay(forAttempt: attempts))
                        await store.recordWorkoutRouteStatus(WorkoutRouteSyncRecord(
                            workoutUUID: candidate.uuid,
                            activityTypeRaw: candidate.activityType.rawValue,
                            workoutStart: candidate.startDate,
                            workoutEnd: candidate.endDate,
                            isExplicitlyIndoor: candidate.isExplicitlyIndoor,
                            status: .pendingLateArrival,
                            attemptCount: attempts,
                            firstSeenAt: firstSeen,
                            lastAttemptAt: now,
                            nextRetryAt: nextRetry
                        ))
                        watermarkHeld = true
                    } else {
                        // Grace window elapsed and HealthKit still has no route for this workout.
                        await store.recordWorkoutRouteStatus(WorkoutRouteSyncRecord(
                            workoutUUID: candidate.uuid,
                            activityTypeRaw: candidate.activityType.rawValue,
                            workoutStart: candidate.startDate,
                            workoutEnd: candidate.endDate,
                            isExplicitlyIndoor: candidate.isExplicitlyIndoor,
                            status: .noRouteInHealthKit,
                            attemptCount: attempts,
                            firstSeenAt: firstSeen,
                            lastAttemptAt: now
                        ))
                        if !watermarkHeld {
                            await store.advanceWorkoutEnrichmentWatermark(.routes, to: candidate.endDate, workouts: 1)
                        }
                    }
                    totalWorkouts += 1
                    continue
                }

                // If this workout is already verified with the exact same point count
                // and we are not forcing a re-upload, skip the redundant upload.
                if let existing,
                   existing.status == .verified,
                   existing.verifiedPointCount >= totalPoints,
                   !forceReupload {
                    if !watermarkHeld {
                        await store.advanceWorkoutEnrichmentWatermark(.routes, to: candidate.endDate, workouts: 0)
                    }
                    totalWorkouts += 1
                    continue
                }

                let isIdempotentReupload = forceReupload
                    || (existing?.status == .verified)
                    || (existing?.status == .uploadFailed && (existing?.attemptCount ?? 0) > 0)
                let reuploadEpoch = forceReupload ? ((existing?.attemptCount ?? 0) + 1) : 0
                let packedChunks = Self.packByPointBudget(payloads, budget: budget)
                let batches = packedChunks.enumerated().map { idx, chunk in
                    let chunkPoints = chunk.reduce(0) { $0 + $1.points.count }
                    let firstTs = chunk.first?.points.first?.t
                    let lastTs = chunk.last?.points.last?.t
                    let batchID = Self.deterministicRouteBatchID(
                        deviceID: deviceID,
                        workoutUUID: candidate.uuid,
                        chunkIndex: idx,
                        pointCount: chunkPoints,
                        firstTimestamp: firstTs,
                        lastTimestamp: lastTs,
                        reuploadEpoch: reuploadEpoch
                    )
                    return SyncBatch(
                        batchID: batchID,
                        deviceID: deviceID,
                        type: typeID,
                        reason: reason,
                        samples: [],
                        deletions: [],
                        routes: chunk,
                        series: []
                    )
                }

                do {
                    try await uploadWorkoutEnrichment(
                        .routes,
                        batches: batches,
                        workoutEnd: candidate.endDate,
                        transport: transport,
                        advanceWatermark: !watermarkHeld,
                        verifyRouteReceipt: !isIdempotentReupload,
                        workoutUUID: candidate.uuid
                    )
                    let attempts = (existing?.attemptCount ?? 0) + 1
                    await store.recordWorkoutRouteStatus(WorkoutRouteSyncRecord(
                        workoutUUID: candidate.uuid,
                        activityTypeRaw: candidate.activityType.rawValue,
                        workoutStart: candidate.startDate,
                        workoutEnd: candidate.endDate,
                        isExplicitlyIndoor: candidate.isExplicitlyIndoor,
                        status: .verified,
                        attemptCount: attempts,
                        firstSeenAt: firstSeen,
                        lastAttemptAt: now,
                        nextRetryAt: nil,
                        expectedPointCount: totalPoints,
                        verifiedPointCount: totalPoints,
                        verifiedAt: now
                    ))
                    totalBatches += batches.count
                    notifyChanged()
                } catch {
                    if error is CancellationError { throw error }
                    let attempts = (existing?.attemptCount ?? 0) + 1
                    let nextRetry = now.addingTimeInterval(WorkoutRouteSyncRecord.retryDelay(forAttempt: attempts))
                    let phase = (error is WorkoutRouteUploadError) ? "receipt_verification" : "route_upload"
                    await store.recordWorkoutRouteStatus(WorkoutRouteSyncRecord(
                        workoutUUID: candidate.uuid,
                        activityTypeRaw: candidate.activityType.rawValue,
                        workoutStart: candidate.startDate,
                        workoutEnd: candidate.endDate,
                        isExplicitlyIndoor: candidate.isExplicitlyIndoor,
                        status: .uploadFailed,
                        attemptCount: attempts,
                        firstSeenAt: firstSeen,
                        lastAttemptAt: now,
                        nextRetryAt: nextRetry,
                        expectedPointCount: totalPoints,
                        verifiedPointCount: existing?.verifiedPointCount ?? 0,
                        verifiedAt: existing?.verifiedAt,
                        lastErrorPhase: phase,
                        lastErrorMessage: error.localizedDescription
                    ))
                    await store.recordWorkoutEnrichmentError(.routes, error: error)
                    await eventLog.log(
                        .error, type: typeID,
                        "Workout route upload failed (\(phase)) for workout \(candidate.uuid): \(error)")
                    watermarkHeld = true
                }
                totalWorkouts += 1
            }

            // Only nudge the watermark to `now` and mark fullPass complete if no
            // workout route is holding the watermark for retry/late arrival.
            if !watermarkHeld {
                await store.advanceWorkoutEnrichmentWatermark(.routes, to: now)
                if fullPass {
                    await store.markWorkoutEnrichmentFullRecompute(.routes, at: now)
                }
            }
            // Keep terminal route records bounded to the last 30 days.
            await store.pruneWorkoutRouteRecords(olderThan: now.addingTimeInterval(-30 * 86_400))

            let elapsed = (ContinuousClock.now - runStart).seconds
            await eventLog.log(
                .info, type: typeID,
                "Workout routes: \(totalWorkouts) workout(s), \(totalBatches) batch(es), \(String(format: "%.1f", elapsed))s\(fullPass ? " (full recompute)" : "")")
            notifyChanged()
        } catch {
            await handleWorkoutEnrichmentFailure(.routes, error: error)
        }
        notifyChanged()
    }

    private func runWorkoutStreamEnrichmentPass(
        from: Date,
        fullPass: Bool,
        budget: Int,
        reason: SyncReason,
        now: Date,
        transport: SyncTransport
    ) async {
        let typeID = HealthTypeCatalog.workoutIdentifier
        let runStart = ContinuousClock.now
        let enricher = SeriesEnricher(healthStore: healthStore)
        do {
            // Ascending by endDate: the watermark advances workout-by-workout, so a
            // mid-phase timeout loses at most the in-flight workout.
            let workouts = try await fetchWorkouts(endedAfter: from)
            var totalWorkouts = 0
            var totalBatches = 0
            for workout in workouts {
                try Task.checkCancellation()
                let batches = try await enrichmentBatches(
                    .streams, for: workout, enricher: enricher, budget: budget, reason: reason)
                if batches.isEmpty {
                    // No streamable curves — trivially complete; still advance the watermark past it.
                    await store.advanceWorkoutEnrichmentWatermark(.streams, to: workout.endDate, workouts: 1)
                } else {
                    try await uploadWorkoutEnrichment(
                        .streams, batches: batches, workoutEnd: workout.endDate, transport: transport)
                    totalBatches += batches.count
                    notifyChanged()
                }
                totalWorkouts += 1
            }
            // Tail: nudge the watermark to now so a later run's lookback starts
            // after the last workout rather than rescanning from its endDate.
            await store.advanceWorkoutEnrichmentWatermark(.streams, to: now)
            if fullPass {
                await store.markWorkoutEnrichmentFullRecompute(.streams, at: now)
            }
            let elapsed = (ContinuousClock.now - runStart).seconds
            await eventLog.log(
                .info, type: typeID,
                "Workout streams: \(totalWorkouts) workout(s), \(totalBatches) batch(es), \(String(format: "%.1f", elapsed))s\(fullPass ? " (full recompute)" : "")")
            notifyChanged()
        } catch {
            await handleWorkoutEnrichmentFailure(.streams, error: error)
        }
        notifyChanged()
    }

    private func handleWorkoutEnrichmentFailure(_ kind: WorkoutEnrichmentKind, error: Error) async {
        let typeID = HealthTypeCatalog.workoutIdentifier
        switch PassFailure(error) {
        case .authorizationNotDetermined:
            await store.recordWorkoutEnrichmentError(kind, error: SyncError.authorizationNotDetermined)
            await eventLog.log(.error, type: typeID, "Workout \(kind.rawValue): Health access not determined")
        case .databaseLocked:
            noteHealthDatabaseLocked()
            // Device locked — expected in background; watermark untouched.
            await eventLog.log(.warn, type: typeID, "Workout \(kind.rawValue): Health database locked — will retry on next wake")
        case .cancelled:
            // Background time expired mid-phase. Not a failure: the watermark
            // sits at the last fully-acked workout and the next wake resumes.
            await eventLog.log(.debug, type: typeID, "Workout \(kind.rawValue): cancelled — will resume on next wake")
        case .failed(let error):
            await store.recordWorkoutEnrichmentError(kind, error: error)
            await eventLog.log(.error, type: typeID, "Workout \(kind.rawValue) sync failed: \(error)")
        }
    }

    /// Build one workout's point-capped enrichment batches (route- or series-only).
    private func enrichmentBatches(
        _ kind: WorkoutEnrichmentKind, for workout: HKWorkout,
        enricher: SeriesEnricher, budget: Int, reason: SyncReason
    ) async throws -> [SyncBatch] {
        let deviceID = store.deviceID
        let typeID = HealthTypeCatalog.workoutIdentifier
        switch kind {
        case .routes:
            guard Self.mayHaveWorkoutRoute(workout.workoutActivityType) else { return [] }
            let payloads = try await enricher.routePayloads(for: workout)
            return Self.packByPointBudget(payloads, budget: budget).map {
                SyncBatch(deviceID: deviceID, type: typeID, reason: reason,
                          samples: [], deletions: [], routes: $0, series: [])
            }
        case .streams:
            let payloads = try await enricher.seriesPayloads(for: workout)
            return Self.packByPointBudget(payloads, budget: budget).map {
                SyncBatch(deviceID: deviceID, type: typeID, reason: reason,
                          samples: [], deletions: [], routes: [], series: $0)
            }
        }
    }

    /// Upload all of one workout's enrichment batches sequentially (low
    /// concurrency, by design — enrichment must not recreate the contention that
    /// starved basic data), recording each, then advance the watermark to
    /// `workoutEnd`. Throws *without advancing* if any chunk fails or if the server
    /// receipt fails route-point verification, so a failure mid-workout leaves the
    /// watermark at the previous fully-acked workout and the next run retries.
    func uploadWorkoutEnrichment(
        _ kind: WorkoutEnrichmentKind,
        batches: [SyncBatch],
        workoutEnd: Date,
        transport: SyncTransport,
        advanceWatermark: Bool = true,
        verifyRouteReceipt: Bool = false,
        workoutUUID: UUID? = nil
    ) async throws {
        for batch in batches {
            try Task.checkCancellation()
            let result = try await transport.upload(batch)
            let routePoints = batch.routes.reduce(0) { $0 + $1.points.count }
            let seriesPoints = batch.series.reduce(0) { $0 + $1.points.count }
            if kind == .routes, routePoints > 0, let acked = result.receipt?.routePoints {
                // Verify the server acknowledged the uploaded route points:
                // - On a fresh upload (`verifyRouteReceipt == true`), `acked` must match `routePoints`.
                // - On an idempotent re-upload (`verifyRouteReceipt == false`), `acked == 0` is valid
                //   when points were already stored (`ON CONFLICT DO NOTHING`) and `acked == routePoints`
                //   is valid when newly inserted, but a partial mismatch (`0 < acked < routePoints`
                //   or `acked > routePoints` or `acked < 0`) still indicates a broken upload.
                let mismatch = verifyRouteReceipt
                    ? (acked != routePoints)
                    : (acked < 0 || acked > routePoints || (acked > 0 && acked < routePoints))
                if mismatch {
                    let targetUUID = workoutUUID ?? batch.routes.first?.workoutUUID ?? UUID()
                    throw WorkoutRouteUploadError.receiptMismatch(
                        workoutUUID: targetUUID,
                        expected: routePoints,
                        acknowledged: acked
                    )
                }
            }
            let points = routePoints + seriesPoints
            await store.recordWorkoutEnrichmentBatch(kind, payloads: points, bytes: result.bytesSent)
            await reportWakeBatch(
                type: HealthTypeCatalog.workoutIdentifier,
                samples: 0,
                deletions: 0,
                bytes: result.bytesSent
            )
        }
        if advanceWatermark {
            await store.advanceWorkoutEnrichmentWatermark(kind, to: workoutEnd, workouts: 1)
        }
    }

    /// All workouts whose `endDate` is at or after `endedAfter`, ascending by end
    /// date. Routes/series aren't anchored, so this is a plain sample query (not an
    /// anchored one) over `HKWorkoutType`; the enrichment phases re-cover a trailing
    /// window each run for self-heal.
    private func fetchWorkouts(endedAfter: Date) async throws -> [HKWorkout] {
        var predicates = [
            HKQuery.predicateForSamples(withStart: endedAfter, end: nil, options: .strictEndDate),
        ]
        if let earliest = await accessPolicy().earliestDate() {
            // Match the raw sweep: a workout crossing the plan boundary is
            // excluded in full, including its older route and sensor points.
            predicates.append(HKQuery.predicateForSamples(
                withStart: earliest, end: nil, options: .strictStartDate))
        }
        if let readEnd {
            // An export's engine: follow only the workouts its raw sweep
            // exported, which are the ones that started before the bound.
            predicates.append(
                HKQuery.predicateForSamples(withStart: nil, end: readEnd, options: .strictStartDate))
        }
        let predicate = HKSamplePredicate<HKSample>.sample(
            type: HKWorkoutType.workoutType(),
            predicate: NSCompoundPredicate(andPredicateWithSubpredicates: predicates)
        )
        let descriptor = HKSampleQueryDescriptor(
            predicates: [predicate],
            sortDescriptors: [SortDescriptor(\.endDate, order: .forward)]
        )
        let results = try await descriptor.result(for: healthStore)
        return results.compactMap { $0 as? HKWorkout }
    }
}
