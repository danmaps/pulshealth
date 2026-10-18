import Foundation
import HealthKit
import CoreLocation

/// Structured error thrown when reading `HKWorkoutRoute` samples or their
/// `CLLocation` stream fails, preserving the workout UUID and the read phase.
public enum WorkoutRouteReadError: Error, LocalizedError, Sendable {
    case routeQueryFailed(workoutUUID: UUID, underlying: Error)
    case locationStreamFailed(workoutUUID: UUID, routeUUID: UUID, underlying: Error)

    public var phase: String {
        switch self {
        case .routeQueryFailed:
            return "route_query"
        case .locationStreamFailed:
            return "route_location_stream"
        }
    }

    public var workoutUUID: UUID {
        switch self {
        case .routeQueryFailed(let workoutUUID, _),
             .locationStreamFailed(let workoutUUID, _, _):
            return workoutUUID
        }
    }

    public var underlyingError: Error {
        switch self {
        case .routeQueryFailed(_, let underlying),
             .locationStreamFailed(_, _, let underlying):
            return underlying
        }
    }

    public var errorDescription: String? {
        switch self {
        case .routeQueryFailed(let workoutUUID, let underlying):
            return "Route sample query failed for workout \(workoutUUID): \(underlying.localizedDescription)"
        case .locationStreamFailed(let workoutUUID, let routeUUID, let underlying):
            return "Route location stream failed for workout \(workoutUUID) (route \(routeUUID)): \(underlying.localizedDescription)"
        }
    }
}

/// Fetches the secondary data attached to series-style samples: heartbeat offsets,
/// ECG voltage traces, workout GPS routes, and workout effort scores. These all
/// require follow-up queries that `SampleMapper` (synchronous) can't run.
struct SeriesEnricher: Sendable {
    let healthStore: HKHealthStore

    // MARK: - Heartbeat series

    func heartbeats(for sample: HKHeartbeatSeriesSample) async throws -> [Heartbeat] {
        var out: [Heartbeat] = []
        for try await beat in HKHeartbeatSeriesQueryDescriptor(sample).results(for: healthStore) {
            out.append(Heartbeat(
                timeSinceSeriesStart: beat.timeIntervalSinceStart,
                precededByGap: beat.precededByGap
            ))
        }
        return out
    }

    // MARK: - ECG voltages

    func voltagesUV(for ecg: HKElectrocardiogram) async throws -> [Double] {
        let microvolt = HKUnit.voltUnit(with: .micro)
        var out: [Double] = []
        out.reserveCapacity(ecg.numberOfVoltageMeasurements)
        for try await measurement in HKElectrocardiogramQueryDescriptor(ecg).results(for: healthStore) {
            if let quantity = measurement.quantity(for: .appleWatchSimilarToLeadI) {
                out.append(quantity.doubleValue(for: microvolt))
            }
        }
        return out
    }

    // MARK: - Workout routes

    /// All GPS points for a workout, split into payloads of at most
    /// `RoutePayload.maxPointsPerPayload` points so a single NDJSON line stays bounded.
    func routePayloads(for workout: HKWorkout) async throws -> [RoutePayload] {
        let routePredicate = HKSamplePredicate<HKSample>.sample(
            type: HKSeriesType.workoutRoute(),
            predicate: HKQuery.predicateForObjects(from: workout)
        )
        let rawRoutes: [HKSample]
        do {
            rawRoutes = try await HKSampleQueryDescriptor(
                predicates: [routePredicate],
                sortDescriptors: [SortDescriptor(\.startDate, order: .forward)]
            ).result(for: healthStore)
        } catch {
            throw WorkoutRouteReadError.routeQueryFailed(workoutUUID: workout.uuid, underlying: error)
        }

        let routes = rawRoutes.compactMap { $0 as? HKWorkoutRoute }.sorted {
            if $0.startDate != $1.startDate { return $0.startDate < $1.startDate }
            return $0.uuid.uuidString < $1.uuid.uuidString
        }

        var points: [RoutePoint] = []
        for route in routes {
            do {
                for try await location in HKWorkoutRouteQueryDescriptor(route).results(for: healthStore) {
                    points.append(RoutePoint(
                        t: location.timestamp,
                        temporalContext: .deviceCurrent(for: location.timestamp),
                        lat: location.coordinate.latitude,
                        lon: location.coordinate.longitude,
                        alt: location.verticalAccuracy >= 0 ? location.altitude : nil,
                        hAcc: location.horizontalAccuracy >= 0 ? location.horizontalAccuracy : nil,
                        vAcc: location.verticalAccuracy >= 0 ? location.verticalAccuracy : nil,
                        speed: location.speed >= 0 ? location.speed : nil,
                        course: location.course >= 0 ? location.course : nil
                    ))
                }
            } catch {
                throw WorkoutRouteReadError.locationStreamFailed(
                    workoutUUID: workout.uuid, routeUUID: route.uuid, underlying: error)
            }
        }
        guard !points.isEmpty else { return [] }
        points.sort { $0.t < $1.t }
        return stride(from: 0, to: points.count, by: RoutePayload.maxPointsPerPayload).map {
            RoutePayload(
                workoutUUID: workout.uuid,
                points: Array(points[$0 ..< min($0 + RoutePayload.maxPointsPerPayload, points.count)])
            )
        }
    }

    // MARK: - Workout series streams

    /// Quantity types whose intra-workout *curve* (vs a single aggregate) is worth
    /// capturing. Only types also present in the catalog (so we have a canonical
    /// unit) and in a given workout's `allStatistics` are actually queried.
    static let streamableTypes: Set<String> = [
        HKQuantityTypeIdentifier.heartRate,
        .activeEnergyBurned, .basalEnergyBurned,
        .distanceWalkingRunning, .distanceCycling, .distanceSwimming,
        .stepCount, .swimmingStrokeCount,
        .runningSpeed, .runningPower, .runningStrideLength,
        .runningVerticalOscillation, .runningGroundContactTime,
        .cyclingSpeed, .cyclingPower, .cyclingCadence,
        .walkingSpeed, .respiratoryRate, .oxygenSaturation,
    ].map(\.rawValue).reduce(into: Set<String>()) { $0.insert($1) }

    /// Errors that mean HealthKit could not be read at all — a locked device
    /// (`errorDatabaseInaccessible`), access never requested
    /// (`errorAuthorizationNotDetermined`), or task cancellation. Enrichment is
    /// otherwise best-effort per workout, but these must abort the phase: treating
    /// them as "no data" would advance the enrichment watermark past workouts that
    /// were never actually read.
    static func isPhaseAbortingError(_ error: Error) -> Bool {
        let unwrapped = (error as? WorkoutRouteReadError)?.underlyingError ?? error
        if unwrapped is CancellationError { return true }
        guard let hk = unwrapped as? HKError else { return false }
        return hk.code == .errorDatabaseInaccessible
            || hk.code == .errorAuthorizationNotDetermined
    }

    /// Per-type time series for a workout, fetched with `HKQuantitySeriesSampleQuery`
    /// (which expands HealthKit's condensed series-backed samples back into the full
    /// curve). Best-effort: a type that errors on its own or has no points is
    /// skipped, but errors that mean the store is unreadable (see
    /// `isPhaseAbortingError`) are rethrown. Long streams are split into
    /// `maxPointsPerPayload`-sized payloads.
    func seriesPayloads(for workout: HKWorkout) async throws -> [WorkoutSeriesPayload] {
        let withinWorkout = HKQuery.predicateForObjects(from: workout)
        var out: [WorkoutSeriesPayload] = []
        for type in workout.allStatistics.keys {
            guard Self.streamableTypes.contains(type.identifier),
                  let descriptor = HealthTypeCatalog.descriptor(for: type.identifier),
                  let unit = descriptor.unit else { continue }
            let predicate = HKSamplePredicate.quantitySample(type: type, predicate: withinWorkout)
            let queryDescriptor = HKQuantitySeriesSampleQueryDescriptor(predicate: predicate, options: [])
            var points: [SeriesPoint] = []
            do {
                for try await result in queryDescriptor.results(for: healthStore)
                where result.quantity.is(compatibleWith: unit) {
                    points.append(SeriesPoint(
                        t: result.dateInterval.start,
                        temporalContext: .deviceCurrent(for: result.dateInterval.start),
                        value: result.quantity.doubleValue(for: unit)
                    ))
                }
            } catch {
                if Self.isPhaseAbortingError(error) { throw error }
                continue
            }
            guard !points.isEmpty else { continue }
            points.sort { $0.t < $1.t }
            for start in stride(from: 0, to: points.count, by: WorkoutSeriesPayload.maxPointsPerPayload) {
                out.append(WorkoutSeriesPayload(
                    workoutUUID: workout.uuid,
                    type: type.identifier,
                    unit: descriptor.unitString,
                    points: Array(points[start ..< min(start + WorkoutSeriesPayload.maxPointsPerPayload, points.count)])
                ))
            }
        }
        return out
    }

    // MARK: - Workout effort (iOS 18)

    /// Effort scores related to a workout, keyed by quantity type identifier in the
    /// same canonical unit the catalog uses ("appleEffortScore").
    @available(iOS 18.0, *)
    func effortScores(for workout: HKWorkout) async -> [String: Double] {
        let descriptor = HKWorkoutEffortRelationshipQueryDescriptor(
            predicate: HKQuery.predicateForObject(with: workout.uuid),
            anchor: nil,
            option: .mostRelevant
        )
        guard let result = try? await descriptor.result(for: healthStore) else { return [:] }
        var out: [String: Double] = [:]
        let unit = HKUnit(from: "appleEffortScore")
        for relationship in result.relationships {
            for case let sample as HKQuantitySample in relationship.samples ?? []
            where sample.quantity.is(compatibleWith: unit) {
                out[sample.quantityType.identifier] = sample.quantity.doubleValue(for: unit)
            }
        }
        return out
    }
}
