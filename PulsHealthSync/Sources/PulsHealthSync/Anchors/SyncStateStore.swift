import Foundation
import os

/// Everything we persist about one type's sync progress. This is what powers the
/// app's per-type debug dashboard.
public struct TypeSyncState: Codable, Sendable, Equatable {
    public var identifier: String
    /// Opaque archived `HKQueryAnchor`. Nil = never synced.
    public var anchorData: Data?
    /// Set once the initial backfill (history before "now" at enable time) has drained.
    public var backfillComplete: Bool
    /// Wall-clock window of exported sample start dates.
    public var earliestExported: Date?
    public var latestExported: Date?
    public var totalSamplesExported: Int
    public var totalDeletionsExported: Int
    public var totalBytesUploaded: Int
    public var totalBatchesUploaded: Int
    public var lastSyncAt: Date?
    public var lastSyncDuration: TimeInterval?
    /// Time from a sample's endDate to its upload, for the most recent incremental batch.
    public var lastObservedLatency: TimeInterval?
    public var lastError: String?
    public var lastErrorAt: Date?
    /// Most recent server reconciliation pass (nil = never reconciled).
    public var lastReconcileAt: Date?
    public var lastReconcileSummary: String?
    /// What the server reported for the most recent batch that carried only
    /// this type (`IngestReceipt`): rows created and UUIDs it already had. Nil
    /// when the server sent no counts, or the batch was shared with other
    /// types and the counts cannot be attributed to this one.
    public var lastBatchAccepted: Int?
    public var lastBatchDuplicates: Int?
    /// The recent-window stream of a type whose backfill has not finished
    /// (`RecentSampleWindow`): an anchor of its own over samples starting at
    /// `recentWindowStart`, so each run sends what is new there before the
    /// oldest-first sweep continues. Separate from `anchorData` by design — a
    /// date-bounded anchor reused for the full sweep would skip everything
    /// older than the window. Both are cleared when the backfill completes.
    public var recentAnchorData: Data?
    public var recentWindowStart: Date?
    /// iOS 27 limited history access: the earliest date HealthKit let this
    /// app read the type from when last asked (`HealthSyncEngine
    /// .refreshReadableHistory`), nil when reading was unlimited — the date
    /// the sync has been running under. When HealthKit later reports an
    /// earlier date, or none, the access has widened, and a type with
    /// progress is re-swept: its anchor sits past everything it read and
    /// would never return the history that just became readable
    /// (`ReadableHistory`). Optional, so state files from 1.5 and earlier
    /// decode as unlimited. Kept through a reset: it describes the grant,
    /// not progress.
    public var readableSince: Date?
    /// Consecutive uploads of this type the server or the network refused in
    /// a way a retry cannot change (`TransportError.startsCooldown`: a 4xx
    /// other than 408/429, an unsupported protocol, a TLS or bad-URL
    /// failure). Nil once one succeeds. Optional, like `cooldownUntil`, so
    /// 1.6 state files decode.
    public var terminalFailures: Int?
    /// Until when the automatic paths leave this type alone after such a
    /// refusal. The anchor has not moved, so every wake would otherwise
    /// re-read, re-encode and re-send the same page only to be refused again.
    /// Grows with `terminalFailures` (`cooldown(afterFailures:)`); a manual
    /// sync ignores it, and an applied configuration change clears it.
    public var cooldownUntil: Date?

    public init(identifier: String) {
        self.identifier = identifier
        self.anchorData = nil
        self.backfillComplete = false
        self.earliestExported = nil
        self.latestExported = nil
        self.totalSamplesExported = 0
        self.totalDeletionsExported = 0
        self.totalBytesUploaded = 0
        self.totalBatchesUploaded = 0
        self.lastSyncAt = nil
        self.lastSyncDuration = nil
        self.lastObservedLatency = nil
        self.lastError = nil
        self.lastErrorAt = nil
    }

    // MARK: Cooldown after a terminal refusal

    /// The first cooldown; each further refusal in a row doubles it.
    public static let cooldownBase: TimeInterval = 15 * 60
    /// Never longer than this, so a type a server-side fix has cured is
    /// picked up again within hours even if nobody taps Sync Now.
    public static let cooldownCap: TimeInterval = 6 * 60 * 60

    /// 15 min, 30 min, 1 h, 2 h, 4 h, then 6 h for every refusal after.
    public static func cooldown(afterFailures failures: Int) -> TimeInterval {
        guard failures > 0 else { return 0 }
        let doublings = Double(min(failures - 1, 16))
        return min(cooldownCap, cooldownBase * pow(2, doublings))
    }

    /// Whether the automatic paths should skip this type at `now`.
    public func isCoolingDown(at now: Date = Date()) -> Bool {
        guard let cooldownUntil else { return false }
        return cooldownUntil > now
    }

    /// Count one more terminal refusal and push the cooldown out.
    mutating func recordTerminalFailure(at now: Date) {
        let failures = (terminalFailures ?? 0) + 1
        terminalFailures = failures
        cooldownUntil = now.addingTimeInterval(Self.cooldown(afterFailures: failures))
    }

    /// An upload went through, or the configuration changed under it.
    mutating func clearCooldown() {
        terminalFailures = nil
        cooldownUntil = nil
    }
}

/// Everything we persist about one aggregate config's sync progress. Aggregates
/// have no `HKQueryAnchor`: progress is a `computedThrough` watermark (a bucket
/// boundary) advanced only after the server acked the upload, mirroring
/// anchor-after-ack for raw samples.
public struct AggregateSyncState: Codable, Sendable, Equatable {
    public var configID: UUID
    /// Buckets ending at or before this are uploaded & acked. Nil = never computed.
    public var computedThrough: Date?
    public var lastComputedAt: Date?
    /// Last time the whole range (from start date) was recomputed; drives the
    /// monthly full pass that repairs edits/deletes older than the lookback.
    public var lastFullRecomputeAt: Date?
    /// Non-nil while an initial or scheduled full pass is in progress. Its
    /// separate cursor is required because `computedThrough` remains the normal
    /// high-water mark during a monthly pass that restarts from the beginning.
    public var fullRecomputeStartedAt: Date?
    public var fullRecomputeThrough: Date?
    public var totalBucketsUploaded: Int
    public var totalBatchesUploaded: Int
    public var totalBytesUploaded: Int
    public var lastError: String?
    public var lastErrorAt: Date?
    /// iOS 27 limited history access: the earliest readable date of the
    /// series' type the last run computed under. Buckets that end before it,
    /// or straddle it, were neither computed nor uploaded, yet the watermark
    /// is a high-water mark past them — so when HealthKit later reports an
    /// earlier date, or none, the run resets the series and recomputes it
    /// from the start date (`ReadableHistory`). Nil = computed unlimited.
    public var readableSince: Date?

    public init(configID: UUID) {
        self.configID = configID
        self.computedThrough = nil
        self.lastComputedAt = nil
        self.lastFullRecomputeAt = nil
        self.fullRecomputeStartedAt = nil
        self.fullRecomputeThrough = nil
        self.totalBucketsUploaded = 0
        self.totalBatchesUploaded = 0
        self.totalBytesUploaded = 0
        self.lastError = nil
        self.lastErrorAt = nil
    }
}

/// Everything we persist about activity-summary (activity rings) sync progress.
/// There is exactly one activity-summary stream per device (unlike aggregates,
/// which are per-config), so this is a singleton. Like aggregates it has no
/// `HKQueryAnchor`: progress is a `computedThrough` day watermark advanced only
/// after the server acked the upload.
public struct ActivitySummaryState: Codable, Sendable, Equatable {
    /// Days at or before this are uploaded & acked. Nil = never computed.
    public var computedThrough: Date?
    public var lastComputedAt: Date?
    /// Last time the whole range was recomputed; drives the monthly full pass.
    public var lastFullRecomputeAt: Date?
    public var totalDaysUploaded: Int
    public var totalBatchesUploaded: Int
    public var totalBytesUploaded: Int
    public var lastError: String?
    public var lastErrorAt: Date?
    /// iOS 27 limited history access: the earliest readable date the last
    /// run read the rings under. Days before it were not uploaded; a widened
    /// grant recomputes every day from the start date. Nil = unlimited.
    public var readableSince: Date?

    public init() {
        self.computedThrough = nil
        self.lastComputedAt = nil
        self.lastFullRecomputeAt = nil
        self.totalDaysUploaded = 0
        self.totalBatchesUploaded = 0
        self.totalBytesUploaded = 0
        self.lastError = nil
        self.lastErrorAt = nil
    }
}

/// Selects which workout-enrichment phase a watermark belongs to. Routes and
/// streams are uploaded in their own late phases (see `WorkoutEnrichmentSync`),
/// each tracking its own singleton `computedThrough` watermark.
public enum WorkoutEnrichmentKind: String, Codable, Sendable, CaseIterable, Hashable {
    case routes
    case streams
}

/// The explicit route-sync state of a workout. Missing routes are never
/// inferred to mean an indoor workout: `indoorWorkout` is set only when
/// HealthKit metadata explicitly reports `HKIndoorWorkout == true`.
public enum WorkoutRouteStatus: String, Codable, Sendable, Equatable {
    /// Route points were read from HealthKit, uploaded, and acknowledged by the server.
    case verified = "verified"
    /// Route-capable workout returned 0 route points while Apple Watch data may still be arriving.
    case pendingLateArrival = "pending_late_route"
    /// Route-capable workout still had 0 route points in HealthKit after the late-arrival grace window.
    case noRouteInHealthKit = "no_route_in_healthkit"
    /// Querying `HKWorkoutRoute` or streaming its `CLLocation` points failed.
    case readFailed = "route_read_failed"
    /// Uploading route batches or verifying the server's route-point receipt failed.
    case uploadFailed = "route_upload_failed"
    /// Workout metadata explicitly set `HKIndoorWorkout == true`.
    case indoorWorkout = "indoor_workout"
}

/// Durable per-workout route enrichment record stored in `WorkoutEnrichmentState.routeRecords`.
public struct WorkoutRouteSyncRecord: Codable, Sendable, Equatable {
    public var workoutUUID: UUID
    public var activityTypeRaw: UInt
    public var workoutStart: Date
    public var workoutEnd: Date
    public var isExplicitlyIndoor: Bool?
    public var status: WorkoutRouteStatus
    public var attemptCount: Int
    public var firstSeenAt: Date
    public var lastAttemptAt: Date
    public var nextRetryAt: Date?
    public var expectedPointCount: Int
    public var verifiedPointCount: Int
    public var verifiedAt: Date?
    public var lastErrorPhase: String?
    public var lastErrorMessage: String?

    public init(
        workoutUUID: UUID,
        activityTypeRaw: UInt,
        workoutStart: Date,
        workoutEnd: Date,
        isExplicitlyIndoor: Bool? = nil,
        status: WorkoutRouteStatus,
        attemptCount: Int = 1,
        firstSeenAt: Date = Date(),
        lastAttemptAt: Date = Date(),
        nextRetryAt: Date? = nil,
        expectedPointCount: Int = 0,
        verifiedPointCount: Int = 0,
        verifiedAt: Date? = nil,
        lastErrorPhase: String? = nil,
        lastErrorMessage: String? = nil
    ) {
        self.workoutUUID = workoutUUID
        self.activityTypeRaw = activityTypeRaw
        self.workoutStart = workoutStart
        self.workoutEnd = workoutEnd
        self.isExplicitlyIndoor = isExplicitlyIndoor
        self.status = status
        self.attemptCount = attemptCount
        self.firstSeenAt = firstSeenAt
        self.lastAttemptAt = lastAttemptAt
        self.nextRetryAt = nextRetryAt
        self.expectedPointCount = expectedPointCount
        self.verifiedPointCount = verifiedPointCount
        self.verifiedAt = verifiedAt
        self.lastErrorPhase = lastErrorPhase
        self.lastErrorMessage = lastErrorMessage
    }

    /// True while the workout still requires a follow-up route query or upload retry.
    public var needsRetry: Bool {
        switch status {
        case .pendingLateArrival, .readFailed, .uploadFailed:
            return true
        case .verified, .noRouteInHealthKit, .indoorWorkout:
            return false
        }
    }

    /// Whether this record is eligible for a retry at `now`.
    public func isRetryDue(at now: Date) -> Bool {
        guard needsRetry else { return false }
        guard let nextRetryAt else { return true }
        return now >= nextRetryAt
    }

    /// Backoff ladder for pending/failed workout routes:
    /// 1m, 2m, 5m, 15m, 30m, then 60m thereafter.
    public static func retryDelay(forAttempt attempt: Int) -> TimeInterval {
        switch max(1, attempt) {
        case 1: return 60
        case 2: return 120
        case 3: return 300
        case 4: return 900
        case 5: return 1_800
        default: return 3_600
        }
    }
}

/// Everything we persist about one workout-enrichment phase's progress (GPS
/// routes or intra-workout streams). Enrichment rides separate NDJSON lines
/// (`{"route":...}` / `{"series":...}`) keyed on the workout UUID, so the basic
/// workout row can land in the raw sweep and its enrichment in a later phase — fully
/// idempotent server-side (`ON CONFLICT DO NOTHING`). Like activity summaries
/// these have no `HKQueryAnchor`: progress is a singleton `computedThrough`
/// watermark (the workout `endDate` covered so far) advanced only after the
/// server acked every chunk of the phase (anchor-after-ack).
public struct WorkoutEnrichmentState: Codable, Sendable, Equatable {
    /// Workouts ending at or before this have had their enrichment uploaded &
    /// acked. Nil = never run.
    public var computedThrough: Date?
    public var lastComputedAt: Date?
    /// Last time the whole range was recomputed; drives the monthly full pass.
    public var lastFullRecomputeAt: Date?
    /// Durable state for an interrupted initial/monthly/forced full pass. This
    /// cursor is independent of the incremental high-water mark above.
    public var fullRecomputeStartedAt: Date?
    public var fullRecomputeThrough: Date?
    /// A reconciliation-requested restart that has not yet reached a guarded
    /// phase boundary. Optional so state files from before this field decode as
    /// no pending request.
    public var forcedRestartPending: Bool?
    public var totalWorkoutsUploaded: Int
    public var totalPayloadsUploaded: Int
    public var totalBatchesUploaded: Int
    public var totalBytesUploaded: Int
    public var lastError: String?
    public var lastErrorAt: Date?
    /// iOS 27 limited history access: the workout type's earliest readable
    /// date the last run enumerated under. Older workouts were invisible to
    /// it, so a widened grant re-enriches from the start date. Nil =
    /// unlimited.
    public var readableSince: Date?
    /// Per-workout route enrichment records, keyed by lowercase workout UUID string.
    /// Optional so state files from earlier versions decode without migration.
    public var routeRecords: [String: WorkoutRouteSyncRecord]?

    public init() {
        self.computedThrough = nil
        self.lastComputedAt = nil
        self.lastFullRecomputeAt = nil
        self.fullRecomputeStartedAt = nil
        self.fullRecomputeThrough = nil
        self.forcedRestartPending = nil
        self.totalWorkoutsUploaded = 0
        self.totalPayloadsUploaded = 0
        self.totalBatchesUploaded = 0
        self.totalBytesUploaded = 0
        self.lastError = nil
        self.lastErrorAt = nil
        self.routeRecords = nil
    }

    /// True when any tracked workout has a pending/failed route whose retry timer is due at `now`.
    public func hasDueRouteRetries(at now: Date = Date()) -> Bool {
        guard let routeRecords else { return false }
        return routeRecords.values.contains { $0.isRetryDue(at: now) }
    }

    /// Earliest `workoutEnd` among non-terminal (`pendingLateArrival`, `readFailed`, `uploadFailed`) workouts.
    public var earliestPendingRouteWorkoutEnd: Date? {
        guard let routeRecords else { return nil }
        return routeRecords.values
            .filter(\.needsRetry)
            .map(\.workoutEnd)
            .min()
    }
}

/// Persists sync configuration and per-type state (anchors, counters) as an
/// atomically-written JSON file in Application Support. An actor so concurrent
/// per-type sync tasks can update state safely.
///
/// The file is protected until first unlock and excluded from backup
/// (`ProtectedStateFile`). It never contains the bearer token: the token lives
/// in the `TokenStore` (the Keychain in production) and is only ever held in
/// memory on `configuration.authToken`.
public actor SyncStateStore {
    public private(set) var configuration: SyncConfiguration
    public private(set) var typeStates: [String: TypeSyncState]
    /// Watermarks/counters per aggregate config, keyed by config UUID string.
    public private(set) var aggregateStates: [String: AggregateSyncState]
    /// Watermark/counters for the singleton activity-summary (rings) stream.
    public private(set) var activitySummaryState: ActivitySummaryState
    /// Watermark/counters for the late workout-route enrichment phase.
    public private(set) var workoutRoutesState: WorkoutEnrichmentState
    /// Watermark/counters for the late workout-stream enrichment phase.
    public private(set) var workoutStreamsState: WorkoutEnrichmentState
    /// The server and user every anchor and watermark above was earned
    /// against. Nil until a server URL has been applied. See `ServerIdentity`.
    public private(set) var serverIdentity: ServerIdentity?
    /// Stable per-install ID sent with every batch.
    public let deviceID: String

    private var destinations: [SavedDestinationState] = []
    private var destinationRegistryInitialized = false
    public private(set) var activeDestinationID: UUID?
    public var savedDestinations: [SavedSyncDestination] { destinations.map(\.destination) }

    private let fileURL: URL
    private let tokenStore: TokenStore
    /// True when `sync-state.json` existed at init but could not be read (see
    /// `init`). The store then holds an empty configuration and, for the rest
    /// of the process, writes nothing: every persist is refused, so the file on
    /// disk is never replaced by that empty state, and the token store is never
    /// written or cleared, since the Keychain item belongs to that file's
    /// configuration — an Apply of the empty one would otherwise delete it.
    public let isReadOnly: Bool
    /// Set when the last hand-off to the token store failed, so the next
    /// configuration write retries instead of assuming the token is safe.
    private var tokenStoreDirty = false
    private var legacyCredentialUnavailable = false
    private let logger = Logger(subsystem: PulsLog.subsystem, category: "state")
    private var saveTask: Task<Void, Never>?

    private struct PersistedState: Codable {
        var configuration: SyncConfiguration
        var typeStates: [String: TypeSyncState]
        var deviceID: String
        var aggregateStates: [String: AggregateSyncState]
        var activitySummaryState: ActivitySummaryState
        var workoutRoutesState: WorkoutEnrichmentState
        var workoutStreamsState: WorkoutEnrichmentState
        var serverIdentity: ServerIdentity?
        /// The bearer token, kept inline ONLY while the token store is
        /// refusing it. Normally absent: the token lives in the Keychain and
        /// `SyncConfiguration` will not encode it. Written when a Keychain
        /// write has failed, so the token survives until the store accepts it —
        /// which is what the migration comment always promised and, until this
        /// field existed, did not deliver: the first `persist()` of the session
        /// rewrote the file without the token and it existed only in memory.
        var fallbackAuthToken: String?
        var destinations: [SavedDestinationState]?
        var activeDestinationID: UUID?

        init(
            configuration: SyncConfiguration,
            typeStates: [String: TypeSyncState],
            deviceID: String,
            aggregateStates: [String: AggregateSyncState],
            activitySummaryState: ActivitySummaryState,
            workoutRoutesState: WorkoutEnrichmentState,
            workoutStreamsState: WorkoutEnrichmentState,
            serverIdentity: ServerIdentity?,
            fallbackAuthToken: String? = nil,
            destinations: [SavedDestinationState]? = nil,
            activeDestinationID: UUID? = nil
        ) {
            self.destinations = destinations
            self.activeDestinationID = activeDestinationID
            self.fallbackAuthToken = fallbackAuthToken
            self.configuration = configuration
            self.typeStates = typeStates
            self.deviceID = deviceID
            self.aggregateStates = aggregateStates
            self.activitySummaryState = activitySummaryState
            self.workoutRoutesState = workoutRoutesState
            self.workoutStreamsState = workoutStreamsState
            self.serverIdentity = serverIdentity
        }

        // The whole file is loaded with `try?` — a synthesized decoder would
        // throw keyNotFound on pre-aggregates state files and silently reset
        // every anchor. Same pattern as SyncConfiguration.
        init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            configuration = try c.decode(SyncConfiguration.self, forKey: .configuration)
            typeStates = try c.decode([String: TypeSyncState].self, forKey: .typeStates)
            deviceID = try c.decode(String.self, forKey: .deviceID)
            aggregateStates = try c.decodeIfPresent(
                [String: AggregateSyncState].self, forKey: .aggregateStates) ?? [:]
            activitySummaryState = try c.decodeIfPresent(
                ActivitySummaryState.self, forKey: .activitySummaryState) ?? ActivitySummaryState()
            workoutRoutesState = try c.decodeIfPresent(
                WorkoutEnrichmentState.self, forKey: .workoutRoutesState) ?? WorkoutEnrichmentState()
            workoutStreamsState = try c.decodeIfPresent(
                WorkoutEnrichmentState.self, forKey: .workoutStreamsState) ?? WorkoutEnrichmentState()
            serverIdentity = try c.decodeIfPresent(ServerIdentity.self, forKey: .serverIdentity)
            fallbackAuthToken = try c.decodeIfPresent(String.self, forKey: .fallbackAuthToken)
            destinations = try c.decodeIfPresent([SavedDestinationState].self, forKey: .destinations)
            activeDestinationID = try c.decodeIfPresent(UUID.self, forKey: .activeDestinationID)
        }
    }

    /// - Parameters:
    ///   - directory: Where `sync-state.json` lives; Application Support by default.
    ///   - tokenStore: Where the bearer token lives; the app Keychain by default.
    ///     Tests and throwaway engines pass an `InMemoryTokenStore`.
    public init(directory: URL? = nil, tokenStore: TokenStore? = nil) {
        let dir = directory ?? FileManager.default
            .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("PulsHealthSync", isDirectory: true)
        ProtectedStateFile.prepareDirectory(dir)
        self.fileURL = dir.appendingPathComponent("sync-state.json")
        let tokenStore = tokenStore ?? KeychainTokenStore()
        self.tokenStore = tokenStore
        let logger = Logger(subsystem: PulsLog.subsystem, category: "state")

        var loaded: PersistedState?
        var unreadable = false
        switch ProtectedStateFile.read(fileURL) {
        case .missing:
            break
        case .unreadable(let error):
            // The file is there but cannot be read — protected data not yet
            // available (a prewarm launch before first unlock), or the path
            // is not a regular file. Starting fresh and persisting would
            // overwrite the only copy of every anchor and watermark, so the
            // store opens empty and read-only for this process: every
            // persist is refused and logged. The app relaunches often
            // enough that the next launch, with the file readable, loads
            // it as usual.
            unreadable = true
            logger.error("Sync state file exists but is unreadable (\(error)); opening read-only, nothing will be persisted this launch")
        case .data(let data):
            do {
                loaded = try JSONDecoder.puls.decode(PersistedState.self, from: data)
            } catch {
                // Starting fresh here would then persist an empty state over the
                // only copy of every anchor and watermark (there are no backups).
                // Move the undecodable file aside so it can be inspected or
                // restored by hand, and only then start clean.
                let quarantine = fileURL.appendingPathExtension(
                    "corrupt-\(Int(Date().timeIntervalSince1970))")
                try? FileManager.default.moveItem(at: fileURL, to: quarantine)
                ProtectedStateFile.protect(quarantine)
                logger.error(
                    "Sync state file undecodable (\(error)); moved aside as \(quarantine.lastPathComponent) and starting fresh")
            }
        }
        self.isReadOnly = unreadable
        if var decoded = loaded {
            var rewrite = false
            if var saved = decoded.destinations {
                for index in saved.indices {
                    if let fallback = saved[index].fallbackAuthToken {
                        do {
                            try tokenStore.setToken(fallback, for: saved[index].destination.id)
                            saved[index].fallbackAuthToken = nil
                            rewrite = true
                        } catch {
                            // Keep the protected fallback and retry next launch.
                        }
                    }
                }
                decoded.destinations = saved
                self.destinations = saved
                self.destinationRegistryInitialized = true
                self.activeDestinationID = decoded.activeDestinationID
                if let active = saved.first(where: { $0.destination.id == decoded.activeDestinationID }) {
                    decoded.configuration.authToken = active.fallbackAuthToken
                        ?? ((try? tokenStore.token(for: active.destination.id)) ?? nil)
                } else {
                    decoded.configuration.authToken = nil
                }
            } else if let legacyToken = decoded.configuration.authToken {
                // One-time migration: builds before the Keychain store kept the
                // token in this file. Move it out and rewrite the file without
                // it. If the Keychain refuses, the file keeps the token for now
                // and the migration retries on the next launch — losing the
                // token would stall every sync until the user re-enters it.
                do {
                    try tokenStore.setToken(legacyToken)
                    rewrite = true
                    logger.notice("Moved the bearer token out of sync-state.json into the token store")
                } catch {
                    tokenStoreDirty = true
                    logger.error("Token store rejected the legacy token; leaving it in the state file for now: \(error)")
                }
            } else if let stranded = decoded.fallbackAuthToken {
                // A previous session's Keychain write failed and the token was
                // parked in the file. Try the store again; either way the token
                // is back in memory, so syncing continues rather than stalling
                // until the user retypes it.
                do {
                    try tokenStore.setToken(stranded)
                    rewrite = true
                    logger.notice("Token store accepted the token parked in sync-state.json; removing it from the file")
                } catch {
                    tokenStoreDirty = true
                    logger.error("Token store still refusing; the token stays in the state file: \(error)")
                }
                decoded.configuration.authToken = stranded
            } else {
                do {
                    decoded.configuration.authToken = try tokenStore.token()
                    if decoded.configuration.authToken == nil,
                       let identity = ServerIdentity(configuration: decoded.configuration) {
                        let id = SavedSyncDestination.identifier(identity: identity, deviceID: decoded.deviceID)
                        decoded.configuration.authToken = try tokenStore.token(for: id)
                    }
                } catch {
                    legacyCredentialUnavailable = true
                    logger.error("Could not read the bearer token from the token store: \(error)")
                }
            }
            if decoded.serverIdentity == nil,
               let adopted = ServerIdentity(configuration: decoded.configuration) {
                // A state file from before identities were recorded: whatever
                // progress it holds was earned against the server it names.
                decoded.serverIdentity = adopted
                rewrite = true
            }
            // Adopt the single-server file without dropping any cursor. A legacy
            // unconfirmed identity edit keeps its old progress labelled with the
            // old identity; it must not be attributed to the edited endpoint.
            if decoded.destinations == nil, !legacyCredentialUnavailable,
               let identity = decoded.serverIdentity,
               identity == ServerIdentity(configuration: decoded.configuration),
               let url = decoded.configuration.serverURL {
                let id = SavedSyncDestination.identifier(identity: identity, deviceID: decoded.deviceID)
                var saved = SavedDestinationState(
                    destination: SavedSyncDestination(id: id, identity: identity, serverURL: url,
                        userID: decoded.configuration.userID),
                    configuration: decoded.configuration, typeStates: decoded.typeStates,
                    aggregateStates: decoded.aggregateStates, activitySummaryState: decoded.activitySummaryState,
                    workoutRoutesState: decoded.workoutRoutesState, workoutStreamsState: decoded.workoutStreamsState)
                do { try tokenStore.setToken(decoded.configuration.authToken, for: id) }
                catch { saved.fallbackAuthToken = decoded.configuration.authToken }
                decoded.destinations = [saved]
                decoded.activeDestinationID = id
                self.destinations = [saved]
                self.destinationRegistryInitialized = true
                self.activeDestinationID = id
                rewrite = true
            }
            self.configuration = decoded.configuration
            self.typeStates = decoded.typeStates
            self.aggregateStates = decoded.aggregateStates
            self.activitySummaryState = decoded.activitySummaryState
            self.workoutRoutesState = decoded.workoutRoutesState
            self.workoutStreamsState = decoded.workoutStreamsState
            self.serverIdentity = decoded.serverIdentity
            self.deviceID = decoded.deviceID
            if rewrite {
                // Can't call the isolated persistNow() from the nonisolated
                // init; the shared writer strips the token the same way — and
                // parks it inline only while the store is refusing it.
                Self.writeSnapshot(
                    decoded, to: fileURL, logger: logger,
                    fallbackToken: tokenStoreDirty ? decoded.configuration.authToken : nil
                )
            }
        } else {
            self.configuration = SyncConfiguration()
            self.typeStates = [:]
            self.aggregateStates = [:]
            self.activitySummaryState = ActivitySummaryState()
            self.workoutRoutesState = WorkoutEnrichmentState()
            self.workoutStreamsState = WorkoutEnrichmentState()
            self.serverIdentity = nil
            self.deviceID = UUID().uuidString
            // A fresh file, but the Keychain may still hold a token from a
            // previous install of the same bundle — reuse it, exactly as the
            // old file-based token would have survived a state reset. (Read
            // only: a read-only store never writes it back or clears it.)
            self.configuration.authToken = (try? tokenStore.token()) ?? nil
        }
    }

    // MARK: - Paired destinations

    private func currentDestinationState(id: UUID) -> SavedDestinationState? {
        guard let identity = serverIdentity ?? ServerIdentity(configuration: configuration),
              var url = configuration.serverURL else { return nil }
        var savedConfiguration = configuration
        if identity != ServerIdentity(configuration: configuration) {
            // Pre-registry unconfirmed URL edits left the old anchors labelled
            // with serverIdentity. Retain those anchors under that identity.
            var components = URLComponents()
            components.scheme = url.scheme ?? "https"
            components.host = identity.host
            components.port = identity.port
            components.path = identity.path
            guard let restored = components.url else { return nil }
            url = restored
            savedConfiguration.serverURL = restored
            savedConfiguration.userID = identity.userID
            savedConfiguration.authToken = nil
        }
        return SavedDestinationState(
            destination: SavedSyncDestination(id: id, identity: identity, serverURL: url, userID: savedConfiguration.userID),
            configuration: savedConfiguration, typeStates: typeStates, aggregateStates: aggregateStates,
            activitySummaryState: activitySummaryState, workoutRoutesState: workoutRoutesState,
            workoutStreamsState: workoutStreamsState)
    }

    private func refreshActiveSnapshot() {
        guard let id = activeDestinationID,
              let index = destinations.firstIndex(where: { $0.destination.id == id }),
              var state = currentDestinationState(id: id) else { return }
        state.fallbackAuthToken = destinations[index].fallbackAuthToken
        destinations[index] = state
    }

    /// Builds a registry without mutating live state. Legacy progress is adopted
    /// exactly once by its existing receiver, never by a newly entered URL.
    private func registrySnapshot() throws -> [SavedDestinationState] {
        var result = destinations
        let identity = serverIdentity ?? ServerIdentity(configuration: configuration)
        let id = activeDestinationID ?? identity.map {
            SavedSyncDestination.identifier(identity: $0, deviceID: deviceID)
        } ?? UUID()
        if var current = currentDestinationState(id: id) {
            if let index = result.firstIndex(where: { $0.destination.id == id }) {
                current.fallbackAuthToken = result[index].fallbackAuthToken
                result[index] = current
            } else {
                do { try tokenStore.setToken(current.configuration.authToken, for: id) }
                catch { current.fallbackAuthToken = current.configuration.authToken }
                result.append(current)
            }
        }
        return result
    }

    private func commitDestination(
        _ target: SavedDestinationState?, configuration newConfiguration: SyncConfiguration,
        registry: [SavedDestinationState]
    ) throws {
        guard !isReadOnly else { throw SavedDestinationError.readOnly }
        let snapshot = PersistedState(
            configuration: newConfiguration, typeStates: target?.typeStates ?? [:], deviceID: deviceID,
            aggregateStates: target?.aggregateStates ?? [:],
            activitySummaryState: target?.activitySummaryState ?? ActivitySummaryState(),
            workoutRoutesState: target?.workoutRoutesState ?? WorkoutEnrichmentState(),
            workoutStreamsState: target?.workoutStreamsState ?? WorkoutEnrichmentState(),
            serverIdentity: target?.destination.identity, destinations: registry,
            activeDestinationID: target?.destination.id)
        do { try ProtectedStateFile.write(JSONEncoder.puls.encode(snapshot), to: fileURL) }
        catch { throw SavedDestinationError.persistence }
        configuration = newConfiguration
        typeStates = snapshot.typeStates
        aggregateStates = snapshot.aggregateStates
        activitySummaryState = snapshot.activitySummaryState
        workoutRoutesState = snapshot.workoutRoutesState
        workoutStreamsState = snapshot.workoutStreamsState
        serverIdentity = snapshot.serverIdentity
        destinations = registry
        destinationRegistryInitialized = true
        activeDestinationID = snapshot.activeDestinationID
        tokenStoreDirty = false
    }

    /// Called only under the engine's configuration exclusion guard.
    func applySavedConfiguration(_ requested: SyncConfiguration, clearAuthToken: Bool = false) throws {
        guard !isReadOnly else { throw SavedDestinationError.readOnly }
        guard !legacyCredentialUnavailable else { throw SavedDestinationError.credential }
        if !FileManager.default.fileExists(atPath: fileURL.path) {
            let checkpoint = PersistedState(
                configuration: configuration, typeStates: typeStates, deviceID: deviceID,
                aggregateStates: aggregateStates, activitySummaryState: activitySummaryState,
                workoutRoutesState: workoutRoutesState, workoutStreamsState: workoutStreamsState,
                serverIdentity: serverIdentity, fallbackAuthToken: tokenStoreDirty ? configuration.authToken : nil)
            do { try ProtectedStateFile.write(JSONEncoder.puls.encode(checkpoint), to: fileURL) }
            catch { throw SavedDestinationError.persistence }
        }
        var registry = try registrySnapshot()
        var config = requested
        guard let identity = ServerIdentity(configuration: config), let url = config.serverURL else {
            config.authToken = nil
            try commitDestination(nil, configuration: config, registry: registry)
            return
        }
        let existing = registry.first { $0.destination.identity == identity }
        if config.authToken == nil, !clearAuthToken, let existing {
            do { config.authToken = try existing.fallbackAuthToken ?? tokenStore.token(for: existing.destination.id) }
            catch { throw SavedDestinationError.credential }
        }
        if clearAuthToken { config.authToken = nil }
        let id = existing?.destination.id ?? SavedSyncDestination.identifier(identity: identity, deviceID: deviceID)
        var target = SavedDestinationState(
            destination: SavedSyncDestination(id: id, identity: identity, serverURL: url, userID: config.userID),
            configuration: config, typeStates: existing?.typeStates ?? [:],
            aggregateStates: existing?.aggregateStates ?? [:],
            activitySummaryState: existing?.activitySummaryState ?? ActivitySummaryState(),
            workoutRoutesState: existing?.workoutRoutesState ?? WorkoutEnrichmentState(),
            workoutStreamsState: existing?.workoutStreamsState ?? WorkoutEnrichmentState())
        // Edits to one aggregate invalidate only that receiver's series.
        for aggregate in config.aggregates {
            if let old = existing?.configuration.aggregates.first(where: { $0.id == aggregate.id }),
               old.seriesIdentity != aggregate.seriesIdentity || old.startDate != aggregate.startDate {
                target.aggregateStates[aggregate.id.uuidString] = AggregateSyncState(configID: aggregate.id)
            }
        }
        let keep = Set(config.aggregates.map { $0.id.uuidString })
        target.aggregateStates = target.aggregateStates.filter { keep.contains($0.key) }
        if config != existing?.configuration {
            for key in target.typeStates.keys { target.typeStates[key]?.clearCooldown() }
        }
        let previousToken: String?
        do {
            previousToken = try existing?.fallbackAuthToken ?? existing?.configuration.authToken
                ?? tokenStore.token(for: id)
        } catch { throw SavedDestinationError.credential }
        let legacyToken: String?
        do { legacyToken = try tokenStore.token() }
        catch { throw SavedDestinationError.credential }
        var changedCredential = false
        do {
            try tokenStore.setToken(config.authToken, for: id)
            changedCredential = true
        } catch {
            // A failed deletion cannot safely forget a still-live credential.
            if clearAuthToken {
                throw SavedDestinationError.credential
            }
            target.fallbackAuthToken = config.authToken
        }
        registry.removeAll { $0.destination.id == id }
        registry.append(target)
        // Retire the obsolete singleton before exposing a different top-level
        // receiver, so an older app cannot send that old secret to this URL.
        do { if legacyToken != nil { try tokenStore.setToken(nil) } }
        catch {
            if changedCredential { try? tokenStore.setToken(previousToken, for: id) }
            throw SavedDestinationError.credential
        }
        do { try commitDestination(target, configuration: config, registry: registry) }
        catch {
            if changedCredential {
                do {
                    try tokenStore.setToken(previousToken, for: id)
                    if legacyToken != nil { try tokenStore.setToken(legacyToken) }
                } catch { throw SavedDestinationError.credential }
            } else {
                do { if legacyToken != nil { try tokenStore.setToken(legacyToken) } }
                catch { throw SavedDestinationError.credential }
            }
            throw error
        }
    }

    func switchDestination(id: UUID) throws {
        guard !isReadOnly else { throw SavedDestinationError.readOnly }
        let registry = try registrySnapshot()
        guard let target = registry.first(where: { $0.destination.id == id }) else { throw SavedDestinationError.notFound }
        var config = target.configuration
        do { config.authToken = try target.fallbackAuthToken ?? tokenStore.token(for: id) }
        catch { throw SavedDestinationError.credential }
        guard config.authToken != nil else { throw SavedDestinationError.credential }
        let legacyToken: String?
        do {
            legacyToken = try tokenStore.token()
            if legacyToken != nil { try tokenStore.setToken(nil) }
        } catch { throw SavedDestinationError.credential }
        do { try commitDestination(target, configuration: config, registry: registry) }
        catch {
            do { if legacyToken != nil { try tokenStore.setToken(legacyToken) } }
            catch { throw SavedDestinationError.credential }
            throw error
        }
    }

    func unpairDestination(id: UUID) throws {
        guard !isReadOnly else { throw SavedDestinationError.readOnly }
        var registry = try registrySnapshot()
        guard let removed = registry.first(where: { $0.destination.id == id }) else { throw SavedDestinationError.notFound }
        let previousToken: String?
        let legacyToken: String?
        do {
            previousToken = try removed.fallbackAuthToken ?? tokenStore.token(for: id)
            legacyToken = try tokenStore.token()
        } catch { throw SavedDestinationError.credential }
        do {
            try tokenStore.setToken(nil, for: id)
            // Every remaining receiver has its own credential now. The legacy
            // singleton may contain an older rotated token, so remove it too.
            if legacyToken != nil { try tokenStore.setToken(nil) }
        } catch {
            try? tokenStore.setToken(previousToken, for: id)
            throw SavedDestinationError.credential
        }
        registry.removeAll { $0.destination.id == id }
        do {
            if id == activeDestinationID {
                var config = configuration
                config.serverURL = nil
                config.authToken = nil
                try commitDestination(nil, configuration: config, registry: registry)
            } else {
                let target = registry.first { $0.destination.id == activeDestinationID }
                try commitDestination(target, configuration: configuration, registry: registry)
            }
        } catch {
            do {
                try tokenStore.setToken(previousToken, for: id)
                if legacyToken != nil { try tokenStore.setToken(legacyToken) }
            } catch { throw SavedDestinationError.credential }
            throw error
        }
    }

    // MARK: - Reads

    public func state(for identifier: String) -> TypeSyncState {
        typeStates[identifier] ?? TypeSyncState(identifier: identifier)
    }

    public func allStates() -> [TypeSyncState] {
        configuration.enabledTypes.sorted().map { state(for: $0) }
    }

    // MARK: - Writes

    /// Replace the configuration. The token goes to the token store; the file
    /// gets everything else.
    ///
    /// **A nil `authToken` keeps the stored token** as long as `config` names
    /// the same database and user (`tokenCarriesOver`): a caller that rebuilds
    /// a configuration without the token must not delete the credential and
    /// stall every sync. Deleting it is `clearAuthToken()`, said on purpose.
    /// A configuration for a different database or user (none included)
    /// without a token still drops the old one: a token is only ever good for
    /// the database that issued it, and must not ride along to another.
    ///
    /// Any change to the configuration lifts every type's cooldown
    /// (`TypeSyncState.cooldownUntil`): the change may be the fix.
    ///
    /// The server identity is recorded from `config` unless applying it would
    /// abandon progress earned against a different server or user
    /// (`serverIdentityChange(applying:)` non-nil) and the caller has not
    /// passed `confirmServerIdentity` — the app asks the user first (start
    /// fresh, or keep progress) and confirms afterwards. Until then the stored
    /// identity stays put, so a launch after an interrupted change still sees
    /// the mismatch and can ask again.
    public func setConfiguration(_ config: SyncConfiguration, confirmServerIdentity: Bool = false) {
        if destinationRegistryInitialized || config.serverURL != nil || configuration.serverURL != nil {
            do { try applySavedConfiguration(config) }
            catch { logger.error("Could not save database settings: \(error.localizedDescription)") }
            return
        }
        var config = config
        if config.authToken == nil, Self.tokenCarriesOver(from: configuration, to: config) {
            config.authToken = configuration.authToken
        }
        let change = serverIdentityChange(applying: config)
        if config.authToken != configuration.authToken || tokenStoreDirty {
            storeToken(config.authToken)
        }
        if config != configuration { clearCooldowns() }
        configuration = config
        if change == nil || confirmServerIdentity, let applied = ServerIdentity(configuration: config) {
            serverIdentity = applied
        }
        persist()
    }

    /// Whether a configuration that carries no token keeps the stored one:
    /// only when it names the same database URL and user ID.
    nonisolated static func tokenCarriesOver(
        from current: SyncConfiguration, to applied: SyncConfiguration
    ) -> Bool {
        applied.serverURL == current.serverURL && applied.userID == current.userID
    }

    /// Replace the bearer token, leaving the rest of the configuration as it
    /// is. Same token-store hand-off (and the same parking in the state file
    /// when the store refuses it) as `setConfiguration`.
    public func setAuthToken(_ token: String) {
        if destinationRegistryInitialized {
            var config = configuration
            config.authToken = token
            do { try applySavedConfiguration(config) }
            catch { logger.error("Could not save database credential: \(error.localizedDescription)") }
            return
        }
        guard token != configuration.authToken || tokenStoreDirty else { return }
        storeToken(token)
        configuration.authToken = token
        clearCooldowns()
        persist()
    }

    /// Delete the bearer token from memory and from the token store — the
    /// one way to do so now that `setConfiguration` keeps it on nil. For a
    /// user who emptied the token field or disconnected the database.
    public func clearAuthToken() {
        if destinationRegistryInitialized {
            do { try applySavedConfiguration(configuration, clearAuthToken: true) }
            catch { logger.error("Could not clear database credential: \(error.localizedDescription)") }
            return
        }
        // Unconditionally: an in-memory nil can also mean the token store
        // could not be read at launch, and the user asked for the item gone.
        storeToken(nil)
        configuration.authToken = nil
        persist()
    }

    /// Lift every type's cooldown.
    private func clearCooldowns() {
        for (identifier, state) in typeStates
        where state.terminalFailures != nil || state.cooldownUntil != nil {
            var cleared = state
            cleared.clearCooldown()
            typeStates[identifier] = cleared
        }
    }

    private func storeToken(_ token: String?) {
        guard !isReadOnly else {
            logger.error("Bearer token not handed to the token store: the store is read-only because its file could not be read at launch")
            return
        }
        do {
            try tokenStore.setToken(token)
            if let id = activeDestinationID {
                try tokenStore.setToken(token, for: id)
                if let index = destinations.firstIndex(where: { $0.destination.id == id }) {
                    destinations[index].fallbackAuthToken = nil
                }
            }
            tokenStoreDirty = false
        } catch {
            tokenStoreDirty = true
            if let index = destinations.firstIndex(where: { $0.destination.id == activeDestinationID }) {
                destinations[index].fallbackAuthToken = token
            }
            logger.error("Could not write the bearer token to the token store: \(error)")
        }
    }

    // MARK: - Server identity

    /// Pure comparison behind `serverIdentityChange(applying:)`: true when both
    /// identities are present and differ. Nothing stored, or a configuration
    /// without a server URL, is never a change.
    public nonisolated static func serverIdentityChanged(
        stored: ServerIdentity?, applied: ServerIdentity?
    ) -> Bool {
        guard let stored, let applied else { return false }
        return stored != applied
    }

    /// True when any anchor or watermark has been earned — i.e. there is
    /// progress a server change could strand.
    public var hasSyncProgress: Bool {
        typeStates.values.contains {
            $0.anchorData != nil || $0.recentAnchorData != nil || $0.totalSamplesExported > 0
        }
            || aggregateStates.values.contains { $0.computedThrough != nil }
            || activitySummaryState.computedThrough != nil
            || workoutRoutesState.computedThrough != nil
            || workoutStreamsState.computedThrough != nil
    }

    /// The change applying `config` would make to the recorded server identity,
    /// or nil when it is the same server and user, no identity is recorded yet,
    /// or there is no progress to strand. A non-nil result means the caller
    /// should ask before `setConfiguration(_:confirmServerIdentity:)`.
    public func serverIdentityChange(applying config: SyncConfiguration) -> ServerIdentityChange? {
        guard let stored = serverIdentity,
              let applied = ServerIdentity(configuration: config),
              Self.serverIdentityChanged(stored: stored, applied: applied),
              hasSyncProgress else { return nil }
        return ServerIdentityChange(from: stored, to: applied)
    }

    /// The mismatch, if any, between the recorded identity and the persisted
    /// configuration itself — the state a launch finds after a change was
    /// applied but never confirmed.
    public func pendingServerIdentityChange() -> ServerIdentityChange? {
        serverIdentityChange(applying: configuration)
    }

    public func update(_ identifier: String, _ mutate: @Sendable (inout TypeSyncState) -> Void) {
        var s = state(for: identifier)
        mutate(&s)
        typeStates[identifier] = s
        persist()
    }

    /// Record a successfully uploaded batch and advance the anchor — one atomic step.
    /// The anchor is only persisted *after* the upload succeeded, so a crash or
    /// failure between query and upload re-exports the page instead of dropping it.
    public func recordUploadedBatch(
        identifier: String,
        newAnchorData: Data?,
        samples: Int,
        deletions: Int,
        bytes: Int,
        sampleDateRange: ClosedRange<Date>?,
        duration: TimeInterval,
        latency: TimeInterval?,
        receipt: IngestReceipt? = nil
    ) {
        update(identifier) { s in
            s.anchorData = newAnchorData
            s.totalSamplesExported += samples
            s.totalDeletionsExported += deletions
            s.totalBytesUploaded += bytes
            s.totalBatchesUploaded += 1
            s.lastSyncAt = Date()
            s.lastSyncDuration = duration
            s.lastError = nil
            s.clearCooldown()
            // Overwritten on every batch, so a server that stopped reporting
            // does not leave a stale count behind.
            s.lastBatchAccepted = receipt?.accepted
            s.lastBatchDuplicates = receipt?.duplicates
            if let latency { s.lastObservedLatency = latency }
            if let range = sampleDateRange {
                s.earliestExported = s.earliestExported.map { min($0, range.lowerBound) } ?? range.lowerBound
                s.latestExported = s.latestExported.map { max($0, range.upperBound) } ?? range.upperBound
            }
        }
    }

    /// `lastError` text: scrubbed of the bearer token, URL queries and control
    /// characters and capped, because it is written to disk and exported with
    /// diagnostics (a server error body can be a whole HTML page).
    private func errorText(_ error: Error) -> String {
        ErrorScrubber.describe(
            error, limit: ErrorScrubber.persistedLimit,
            secrets: [configuration.authToken].compactMap { $0 })
    }

    /// Record a failed run. An upload refused in a way no retry can change
    /// (`TransportError.startsCooldown`) also starts or lengthens the type's
    /// cooldown; anything else — HealthKit, a network outage, a 5xx — does
    /// not, and the next wake tries again as before.
    public func recordError(identifier: String, error: Error, at now: Date = Date()) {
        let text = errorText(error)
        let terminal = (error as? TransportError)?.startsCooldown == true
        update(identifier) { s in
            s.lastError = text
            s.lastErrorAt = now
            if terminal { s.recordTerminalFailure(at: now) }
        }
    }

    /// Record an acked page of a type's recent-window stream. The stream's own
    /// anchor moves; `anchorData` and `backfillComplete` never do — the sweep
    /// has not reached these samples, and will send them again when it does.
    /// Traffic and date coverage are real, so those advance; the sample count
    /// is left to the sweep, which would otherwise count these twice.
    public func recordRecentWindowUpload(
        identifier: String,
        newAnchorData: Data?,
        windowStart: Date,
        bytes: Int,
        sampleDateRange: ClosedRange<Date>?,
        duration: TimeInterval
    ) {
        update(identifier) { s in
            s.recentAnchorData = newAnchorData
            s.recentWindowStart = windowStart
            s.totalBytesUploaded += bytes
            s.totalBatchesUploaded += 1
            s.lastSyncAt = Date()
            s.lastSyncDuration = duration
            s.lastError = nil
            s.clearCooldown()
            if let range = sampleDateRange {
                s.earliestExported = s.earliestExported.map { min($0, range.lowerBound) } ?? range.lowerBound
                s.latestExported = s.latestExported.map { max($0, range.upperBound) } ?? range.upperBound
            }
        }
    }

    /// The backfill is done, so the recent-window stream has nothing left to
    /// get ahead of: from here the type's own anchor carries everything new.
    public func markBackfillComplete(_ identifier: String) {
        update(identifier) { s in
            s.backfillComplete = true
            s.recentAnchorData = nil
            s.recentWindowStart = nil
        }
    }

    /// Note the earliest date HealthKit now lets the app read a type from
    /// (iOS 27 limited history access), nil for unlimited. Only for a change
    /// that needs no re-sweep — see `restartBackfillForWidenedAccess`.
    public func recordReadableSince(_ identifier: String, _ date: Date?) {
        update(identifier) { $0.readableSince = date }
    }

    /// Access to a type widened while its sync had been reading under a
    /// limit: start its history over. Both anchors and the recent-window
    /// stream go and the backfill is no longer complete, so the next sweep
    /// reads everything from the start date — including what was readable
    /// before, which the server ignores (`ON CONFLICT DO NOTHING` on UUID).
    /// The sample count restarts with it, since that sweep counts every
    /// sample again; traffic and dates are history and stay.
    ///
    /// The caller must hold the type's claim, for the reason
    /// `HealthSyncEngine.resetType` gives.
    public func restartBackfillForWidenedAccess(_ identifier: String, readableSince: Date?) {
        update(identifier) { s in
            s.anchorData = nil
            s.recentAnchorData = nil
            s.recentWindowStart = nil
            s.backfillComplete = false
            s.totalSamplesExported = 0
            s.lastSyncAt = nil
            // In the same write as the reset: a crash that loses one loses
            // both, and the next refresh sees the widening again.
            s.readableSince = readableSince
        }
    }

    /// A stored anchor that will not decode — a restored or downgraded state
    /// file, a blob written by another OS — would fail the type on every run
    /// until someone found Reset. Start that stream over instead. Its bytes
    /// are never looked at: an `HKQueryAnchor` is opaque.
    ///
    /// For the main anchor this is the same restart as a widened grant
    /// (`restartBackfillForWidenedAccess`) with the grant left as it is: both
    /// anchors and the recent-window stream go and the backfill reopens.
    /// Re-sending what the server already has is a no-op there (`ON CONFLICT
    /// DO NOTHING` on UUID). For the recent-window stream only its own anchor
    /// goes; the next recent pass re-reads its window.
    ///
    /// The caller must hold the type's claim, for the reason
    /// `HealthSyncEngine.resetType` gives.
    public func restartForUnreadableAnchor(_ identifier: String, recentStreamOnly: Bool) {
        update(identifier) { s in
            if recentStreamOnly {
                s.recentAnchorData = nil
                return
            }
            s.anchorData = nil
            s.recentAnchorData = nil
            s.recentWindowStart = nil
            s.backfillComplete = false
            s.totalSamplesExported = 0
            s.lastSyncAt = nil
        }
    }

    public func recordReconciliation(identifier: String, summary: String) {
        update(identifier) { s in
            s.lastReconcileAt = Date()
            s.lastReconcileSummary = summary
        }
    }

    /// Reset a type back to "never synced" (drops the anchor; next sync re-exports everything).
    /// The type's `readableSince` survives: it describes what iOS lets the
    /// app read, which a reset does not change, and the backfill that follows
    /// runs under it.
    public func resetType(_ identifier: String) {
        var fresh = TypeSyncState(identifier: identifier)
        fresh.readableSince = typeStates[identifier]?.readableSince
        typeStates[identifier] = fresh
        persist()
    }

    public func resetAll() {
        // Every type's progress goes; what iOS lets the app read stays, for
        // the reason `resetType` gives.
        typeStates = typeStates.compactMapValues { state in
            state.readableSince.map { since in
                var fresh = TypeSyncState(identifier: state.identifier)
                fresh.readableSince = since
                return fresh
            }
        }
        aggregateStates = [:]
        activitySummaryState = ActivitySummaryState()
        workoutRoutesState = WorkoutEnrichmentState()
        workoutStreamsState = WorkoutEnrichmentState()
        persist()
    }

    // MARK: - Aggregate state

    public func aggregateState(for configID: UUID) -> AggregateSyncState {
        aggregateStates[configID.uuidString] ?? AggregateSyncState(configID: configID)
    }

    public func updateAggregate(
        _ configID: UUID, _ mutate: @Sendable (inout AggregateSyncState) -> Void
    ) {
        var s = aggregateState(for: configID)
        mutate(&s)
        aggregateStates[configID.uuidString] = s
        persist()
    }

    /// Record an acked aggregate upload and advance the watermark — one atomic
    /// step, like `recordUploadedBatch` for raw samples. The watermark moves
    /// only *after* the server confirmed the chunk; recomputed buckets are
    /// upserts server-side, so re-sending after a crash is harmless.
    public func recordAggregateUpload(
        configID: UUID,
        newComputedThrough: Date,
        buckets: Int,
        bytes: Int
    ) {
        updateAggregate(configID) { s in
            // A trailing-lookback recompute can end before the stored watermark.
            s.computedThrough = max(s.computedThrough ?? .distantPast, newComputedThrough)
            if s.fullRecomputeStartedAt != nil {
                s.fullRecomputeThrough = max(
                    s.fullRecomputeThrough ?? .distantPast, newComputedThrough)
            }
            s.lastComputedAt = Date()
            s.totalBucketsUploaded += buckets
            s.totalBatchesUploaded += 1
            s.totalBytesUploaded += bytes
            s.lastError = nil
        }
    }

    /// Record an aggregate upload that must move no watermark.
    ///
    /// The priority pass covers a recent window before the full pass has run, so
    /// its chunks end near *now*. Feeding those ends to `recordAggregateUpload`
    /// would push `computedThrough` — and, mid-full-pass, `fullRecomputeThrough`
    /// — past the whole unprocessed history, and the full pass would then
    /// compute nothing older than the window. The counters are still real bytes
    /// and buckets that went to the server, so those advance.
    public func recordAggregateUploadWithoutWatermark(
        configID: UUID,
        buckets: Int,
        bytes: Int
    ) {
        updateAggregate(configID) { s in
            s.lastComputedAt = Date()
            s.totalBucketsUploaded += buckets
            s.totalBatchesUploaded += 1
            s.totalBytesUploaded += bytes
            s.lastError = nil
        }
    }

    public func recordAggregateError(configID: UUID, error: Error) {
        let text = errorText(error)
        updateAggregate(configID) { s in
            s.lastError = text
            s.lastErrorAt = Date()
        }
    }

    /// Persist the start of a full pass before its first query/upload. A pass that
    /// is already in progress keeps its cursor.
    public func beginAggregateFullRecompute(
        configID: UUID, at date: Date = Date(), resumeThrough: Date? = nil
    ) {
        updateAggregate(configID) { s in
            if s.fullRecomputeStartedAt == nil {
                s.fullRecomputeStartedAt = date
                s.fullRecomputeThrough = resumeThrough
            }
        }
        // This marker must survive termination between the first query and the
        // first ack. Full passes are rare, so bypass the normal write debounce.
        persistNow()
    }

    public func markAggregateFullRecompute(configID: UUID, at date: Date = Date()) {
        updateAggregate(configID) { s in
            s.lastFullRecomputeAt = date
            s.fullRecomputeStartedAt = nil
            s.fullRecomputeThrough = nil
        }
        persistNow()
    }

    /// Drop the watermark so the next sync recomputes the whole series ("Recompute
    /// All", or an identity edit that makes this a different server series).
    public func resetAggregate(configID: UUID) {
        aggregateStates[configID.uuidString] = AggregateSyncState(configID: configID)
        persist()
    }

    /// Drop state for deleted configs so it doesn't accumulate forever.
    public func pruneAggregateStates(keeping ids: Set<UUID>) {
        let keep = Set(ids.map(\.uuidString))
        let before = aggregateStates.count
        aggregateStates = aggregateStates.filter { keep.contains($0.key) }
        if aggregateStates.count != before { persist() }
    }

    // MARK: - Activity-summary state

    public func updateActivitySummary(
        _ mutate: @Sendable (inout ActivitySummaryState) -> Void
    ) {
        var s = activitySummaryState
        mutate(&s)
        activitySummaryState = s
        persist()
    }

    /// Record an acked activity-summary upload and advance the day watermark —
    /// one atomic step, like `recordAggregateUpload`. The watermark moves only
    /// *after* the server confirmed the upload; summaries are upserts
    /// server-side, so re-sending after a crash is harmless.
    public func recordActivitySummaryUpload(
        newComputedThrough: Date, days: Int, bytes: Int
    ) {
        updateActivitySummary { s in
            // A trailing-lookback recompute can end before the stored watermark.
            s.computedThrough = max(s.computedThrough ?? .distantPast, newComputedThrough)
            s.lastComputedAt = Date()
            s.totalDaysUploaded += days
            s.totalBatchesUploaded += 1
            s.totalBytesUploaded += bytes
            s.lastError = nil
        }
    }

    public func recordActivitySummaryError(error: Error) {
        let text = errorText(error)
        updateActivitySummary { s in
            s.lastError = text
            s.lastErrorAt = Date()
        }
    }

    public func markActivitySummaryFullRecompute(at date: Date = Date()) {
        updateActivitySummary { s in s.lastFullRecomputeAt = date }
    }

    /// Drop the watermark so the next sync recomputes every day from the start date.
    public func resetActivitySummary() {
        activitySummaryState = ActivitySummaryState()
        persist()
    }

    // MARK: - Workout-enrichment state (routes / streams)

    public func workoutEnrichmentState(_ kind: WorkoutEnrichmentKind) -> WorkoutEnrichmentState {
        switch kind {
        case .routes: return workoutRoutesState
        case .streams: return workoutStreamsState
        }
    }

    private func setWorkoutEnrichmentState(_ kind: WorkoutEnrichmentKind, _ s: WorkoutEnrichmentState) {
        switch kind {
        case .routes: workoutRoutesState = s
        case .streams: workoutStreamsState = s
        }
    }

    public func updateWorkoutEnrichment(
        _ kind: WorkoutEnrichmentKind, _ mutate: @Sendable (inout WorkoutEnrichmentState) -> Void
    ) {
        var s = workoutEnrichmentState(kind)
        mutate(&s)
        setWorkoutEnrichmentState(kind, s)
        persist()
    }

    /// Record one acked enrichment batch (counters + bytes). The watermark is
    /// *not* advanced here: a single workout's points can span several batches and
    /// the watermark must only move once *every* chunk of that workout is acked
    /// (anchor-after-ack) — see `advanceWorkoutEnrichmentWatermark`. Re-sending a
    /// chunk after a crash is harmless: routes/series are UUID-keyed
    /// `ON CONFLICT DO NOTHING`.
    public func recordWorkoutEnrichmentBatch(
        _ kind: WorkoutEnrichmentKind, payloads: Int, bytes: Int
    ) {
        updateWorkoutEnrichment(kind) { s in
            s.lastComputedAt = Date()
            s.totalPayloadsUploaded += payloads
            s.totalBatchesUploaded += 1
            s.totalBytesUploaded += bytes
            s.lastError = nil
        }
    }

    /// Advance the enrichment watermark to a fully-acked workout's `endDate`
    /// (workouts are processed in ascending end-date order, so this only ever
    /// moves forward — `max` also guards the trailing-lookback re-run case).
    /// Advancing *per workout* (not just at end of phase) means a mid-phase
    /// failure loses at most the in-flight workout, not the whole backfill.
    public func advanceWorkoutEnrichmentWatermark(
        _ kind: WorkoutEnrichmentKind, to newComputedThrough: Date, workouts: Int = 0
    ) {
        updateWorkoutEnrichment(kind) { s in
            s.computedThrough = max(s.computedThrough ?? .distantPast, newComputedThrough)
            if s.fullRecomputeStartedAt != nil {
                s.fullRecomputeThrough = max(
                    s.fullRecomputeThrough ?? .distantPast, newComputedThrough)
            }
            s.lastComputedAt = Date()
            s.totalWorkoutsUploaded += workouts
            s.lastError = nil
        }
    }

    public func recordWorkoutEnrichmentError(_ kind: WorkoutEnrichmentKind, error: Error) {
        let text = errorText(error)
        updateWorkoutEnrichment(kind) { s in
            s.lastError = text
            s.lastErrorAt = Date()
        }
    }

    /// Durably queue every requested phase in one actor mutation + atomic file
    /// write. Reconciliation uses this to queue routes and streams before either
    /// phase starts, so termination during routes cannot lose the streams work.
    public func requestForcedWorkoutEnrichmentFullRecompute(
        _ kinds: Set<WorkoutEnrichmentKind>
    ) {
        guard !kinds.isEmpty else { return }
        for kind in kinds {
            var state = workoutEnrichmentState(kind)
            state.forcedRestartPending = true
            setWorkoutEnrichmentState(kind, state)
        }
        persistNow()
    }

    /// At a guarded run boundary, atomically turn pending intent into a durable
    /// fresh full-pass marker. If the process dies after this returns, the marker
    /// resumes from the beginning on the next ordinary phase run.
    public func consumeForcedWorkoutEnrichmentFullRecompute(
        _ kind: WorkoutEnrichmentKind, at date: Date = Date()
    ) -> Bool {
        var state = workoutEnrichmentState(kind)
        guard state.forcedRestartPending == true else { return false }
        state.forcedRestartPending = nil
        state.fullRecomputeStartedAt = date
        state.fullRecomputeThrough = nil
        setWorkoutEnrichmentState(kind, state)
        persistNow()
        return true
    }

    public func hasForcedWorkoutEnrichmentFullRecompute(
        _ kind: WorkoutEnrichmentKind
    ) -> Bool {
        workoutEnrichmentState(kind).forcedRestartPending == true
    }

    /// Persist a full-pass marker before querying, preserving an in-progress
    /// marker and cursor. Forced restarts exclusively use the durable
    /// request/consume operations above.
    public func beginWorkoutEnrichmentFullRecompute(
        _ kind: WorkoutEnrichmentKind, at date: Date = Date(), resumeThrough: Date? = nil
    ) {
        updateWorkoutEnrichment(kind) { s in
            if s.fullRecomputeStartedAt == nil {
                s.fullRecomputeStartedAt = date
                s.fullRecomputeThrough = resumeThrough
            }
        }
        persistNow()
    }

    public func markWorkoutEnrichmentFullRecompute(_ kind: WorkoutEnrichmentKind, at date: Date = Date()) {
        updateWorkoutEnrichment(kind) { s in
            s.lastFullRecomputeAt = date
            s.fullRecomputeStartedAt = nil
            s.fullRecomputeThrough = nil
        }
        persistNow()
    }

    /// Drop the watermark so the next run re-enriches every workout from the start date.
    public func resetWorkoutEnrichment(_ kind: WorkoutEnrichmentKind) {
        setWorkoutEnrichmentState(kind, WorkoutEnrichmentState())
        persist()
    }

    // MARK: - Per-workout route status tracking

    public func workoutRouteRecord(for workoutUUID: UUID) -> WorkoutRouteSyncRecord? {
        workoutRoutesState.routeRecords?[workoutUUID.uuidString.lowercased()]
    }

    public func allWorkoutRouteRecords() -> [UUID: WorkoutRouteSyncRecord] {
        guard let records = workoutRoutesState.routeRecords else { return [:] }
        var out: [UUID: WorkoutRouteSyncRecord] = [:]
        for record in records.values {
            out[record.workoutUUID] = record
        }
        return out
    }

    public func recordWorkoutRouteStatus(_ record: WorkoutRouteSyncRecord) {
        var scrubbed = record
        if let msg = scrubbed.lastErrorMessage {
            scrubbed.lastErrorMessage = ErrorScrubber.scrub(
                msg, limit: ErrorScrubber.persistedLimit,
                secrets: [configuration.authToken].compactMap { $0 })
        }
        updateWorkoutEnrichment(.routes) { s in
            var map = s.routeRecords ?? [:]
            map[scrubbed.workoutUUID.uuidString.lowercased()] = scrubbed
            s.routeRecords = map
        }
    }

    public func dueWorkoutRouteRetryRecords(at now: Date = Date()) -> [WorkoutRouteSyncRecord] {
        guard let records = workoutRoutesState.routeRecords else { return [] }
        return records.values
            .filter { $0.isRetryDue(at: now) }
            .sorted { $0.workoutEnd < $1.workoutEnd }
    }

    public func earliestPendingWorkoutRouteEnd() -> Date? {
        workoutRoutesState.earliestPendingRouteWorkoutEnd
    }

    /// Prune terminal route records (`verified`, `noRouteInHealthKit`, `indoorWorkout`)
    /// whose workout ended before `cutoff`, while retaining all pending/failed records.
    public func pruneWorkoutRouteRecords(olderThan cutoff: Date) {
        updateWorkoutEnrichment(.routes) { s in
            guard let records = s.routeRecords, !records.isEmpty else { return }
            let filtered = records.filter { _, record in
                record.needsRetry || record.workoutEnd >= cutoff
            }
            if filtered.count != records.count {
                s.routeRecords = filtered.isEmpty ? nil : filtered
            }
        }
    }

    // MARK: - Persistence

    /// Debounced atomic write. State changes arrive at high frequency during backfill;
    /// coalescing keeps disk I/O off the critical path while staying crash-safe
    /// (worst case we re-upload one already-uploaded page, which the server dedupes by UUID).
    private func persist() {
        // persistNow() refuses and logs; no need to arm a timer for that on
        // every state change.
        guard !isReadOnly, saveTask == nil else { return }
        saveTask = Task {
            try? await Task.sleep(for: .milliseconds(250))
            saveTask = nil
            persistNow()
        }
    }

    public func persistNow() {
        guard !isReadOnly else {
            logger.error("Sync state not persisted: the store is read-only because its file could not be read at launch")
            return
        }
        refreshActiveSnapshot()
        let snapshot = PersistedState(
            configuration: configuration, typeStates: typeStates, deviceID: deviceID,
            aggregateStates: aggregateStates, activitySummaryState: activitySummaryState,
            workoutRoutesState: workoutRoutesState, workoutStreamsState: workoutStreamsState,
            serverIdentity: serverIdentity,
            destinations: destinationRegistryInitialized ? destinations : nil,
            activeDestinationID: activeDestinationID
        )
        Self.writeSnapshot(
            snapshot, to: fileURL, logger: logger,
            fallbackToken: tokenStoreDirty ? configuration.authToken : nil
        )
    }

    /// The one path to disk. The token is stripped here (belt and braces —
    /// `SyncConfiguration` refuses to encode it anyway) and the file is written
    /// atomically with protection and backup exclusion.
    private nonisolated static func writeSnapshot(
        _ snapshot: PersistedState, to url: URL, logger: Logger,
        fallbackToken: String? = nil
    ) {
        var snapshot = snapshot
        snapshot.configuration.authToken = nil
        // Only non-nil when the token store has refused the token; otherwise
        // the field is omitted entirely and the Keychain remains the one copy.
        snapshot.fallbackAuthToken = fallbackToken
        do {
            let data = try JSONEncoder.puls.encode(snapshot)
            try ProtectedStateFile.write(data, to: url)
        } catch {
            logger.error("Failed to persist sync state: \(error)")
        }
    }
}

extension JSONEncoder {
    /// Shared wire/persistence encoder: epoch-milliseconds dates (fast + compact).
    static var puls: JSONEncoder {
        let e = JSONEncoder()
        e.dateEncodingStrategy = .millisecondsSince1970
        return e
    }
}

extension JSONDecoder {
    static var puls: JSONDecoder {
        let d = JSONDecoder()
        d.dateDecodingStrategy = .millisecondsSince1970
        return d
    }
}
