package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
	// No user lives here: every read takes the user it is for as an
	// argument, settled per request by the scopeUser middleware in main.go
	// (the ?user= parameter, else PULS_USER_ID). One Store serves everyone
	// the database holds.
	// Deployment fallback retained for startup and legacy configuration.
	// Calendar reads resolve the selected account through UserLocation.
	loc *time.Location
	// The clock the summary's "today" is read from; time.Now outside tests.
	now func() time.Time
}

func NewStore(pool *pgxpool.Pool, loc *time.Location) *Store {
	if loc == nil {
		loc = time.UTC
	}
	return &Store{pool: pool, loc: loc, now: time.Now}
}

func (st *Store) Ping(ctx context.Context) error { return st.pool.Ping(ctx) }

type Profile struct {
	TimeZone      string  `json:"timeZone"`
	UserID        string  `json:"userID"`
	Name          *string `json:"name"`
	Email         *string `json:"email"`
	DateOfBirth   *int64  `json:"dateOfBirth"`
	BiologicalSex *string `json:"biologicalSex"`
}

type CatalogType struct {
	Identifier    string  `json:"identifier"`
	Kind          string  `json:"kind"`
	Unit          *string `json:"unit"`
	Rows          int64   `json:"rows"`
	RawRows       int64   `json:"rawRows"`
	AggregateRows int64   `json:"aggregateRows"`
	Earliest      *int64  `json:"earliest"`
	Latest        *int64  `json:"latest"`
}

type LatestMetric struct {
	Identifier string   `json:"identifier"`
	Unit       *string  `json:"unit"`
	Value      *float64 `json:"value"`
	Timestamp  int64    `json:"timestamp"`
}

type DailyPoint struct {
	Date  string   `json:"date"`
	Value *float64 `json:"value"`
}

type DailyMetric struct {
	Identifier string       `json:"identifier"`
	Unit       *string      `json:"unit"`
	Days       []DailyPoint `json:"days"`
}

// DailyFilters is one GET /v1/metrics/daily query. The page is counted in
// days (one metric_daily row), not metrics: Limit and Offset walk the rows
// in the order the response nests them — the requested types in request
// order, each ascending by day — so a page boundary can fall inside a
// metric's days and the next page continues that metric. Limit zero or less
// means no limit (the export's whole range, see nullableLimit).
type DailyFilters struct {
	Types  []string
	Start  time.Time
	End    time.Time
	Limit  int
	Offset int
}

// Points is how many days a DailyMetrics answer carries across its metrics —
// the row count the page's nextOffset advances by.
func (m DailyMetric) points() int { return len(m.Days) }

type ActivityDay struct {
	Date            string   `json:"date"`
	MoveKcal        *float64 `json:"moveKcal"`
	MoveGoalKcal    *float64 `json:"moveGoalKcal"`
	ExerciseMin     *float64 `json:"exerciseMin"`
	ExerciseGoalMin *float64 `json:"exerciseGoalMin"`
	StandHours      *float64 `json:"standHours"`
	StandGoalHours  *float64 `json:"standGoalHours"`
	MoveMode        *int     `json:"moveMode"`
	MoveTimeMin     *float64 `json:"moveTimeMin"`
	MoveTimeGoalMin *float64 `json:"moveTimeGoalMin"`
}

type WorkoutFilters struct {
	Start        *time.Time
	End          *time.Time
	ActivityType string
	Limit        int
	Offset       int
}

type WorkoutSummary struct {
	UUID               string   `json:"uuid"`
	ActivityType       string   `json:"activityType"`
	Start              int64    `json:"start"`
	End                int64    `json:"end"`
	DurationS          *float64 `json:"durationS"`
	DistanceM          *float64 `json:"distanceM"`
	EnergyKcal         *float64 `json:"energyKcal"`
	HasRoute           bool     `json:"hasRoute"`
	RoutePointCount    int64    `json:"routePointCount"`
	IsIndoor           *bool    `json:"isIndoor,omitempty"`
	ElevationAscendedM *float64 `json:"elevationAscendedM,omitempty"`
	AvailableMetrics   []string `json:"availableMetrics"`
}

type WorkoutStatDetail struct {
	Min *float64 `json:"min,omitempty"`
	Avg *float64 `json:"avg,omitempty"`
	Max *float64 `json:"max,omitempty"`
	Sum *float64 `json:"sum,omitempty"`
}

type WorkoutDetail struct {
	WorkoutSummary
	StatisticsDetail map[string]WorkoutStatDetail `json:"statisticsDetail,omitempty"`
	Events           []map[string]any             `json:"events,omitempty"`
	Activities       []map[string]any             `json:"activities,omitempty"`
}

func (st *Store) Profile(ctx context.Context, userID string) (*Profile, error) {
	row := st.pool.QueryRow(ctx, `
		SELECT id::text, name, email,
		       (extract(epoch FROM (dob::timestamp AT TIME ZONE 'UTC')) * 1000)::bigint,
		       biological_sex, puls_user_time_zone(id)
		FROM users
		WHERE id = $1`, userID)

	var profile Profile
	if err := row.Scan(
		&profile.UserID,
		&profile.Name,
		&profile.Email,
		&profile.DateOfBirth,
		&profile.BiologicalSex,
		&profile.TimeZone,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &profile, nil
}

func (st *Store) CatalogTypes(ctx context.Context, userID string) ([]CatalogType, error) {
	rows, err := st.pool.Query(ctx, `
		WITH per_table AS (
			SELECT st.identifier, st.kind, st.unit,
			       count(*)::bigint AS raw_rows, 0::bigint AS aggregate_rows,
			       (extract(epoch FROM min(q.start_ts)) * 1000)::bigint AS earliest,
			       (extract(epoch FROM max(q.start_ts)) * 1000)::bigint AS latest
			FROM quantity_samples q
			JOIN sample_types st USING (type_id)
			WHERE q.user_id = $1
			GROUP BY st.identifier, st.kind, st.unit
			UNION ALL
			SELECT st.identifier, st.kind, st.unit, count(*)::bigint, 0::bigint,
			       (extract(epoch FROM min(c.start_ts)) * 1000)::bigint,
			       (extract(epoch FROM max(c.start_ts)) * 1000)::bigint
			FROM category_samples c
			JOIN sample_types st USING (type_id)
			WHERE c.user_id = $1
			GROUP BY st.identifier, st.kind, st.unit
			UNION ALL
			SELECT 'HKWorkoutTypeIdentifier', 'workout', NULL::text, count(*)::bigint, 0::bigint,
			       (extract(epoch FROM min(start_ts)) * 1000)::bigint,
			       (extract(epoch FROM max(start_ts)) * 1000)::bigint
			FROM workouts
			WHERE user_id = $1
			HAVING count(*) > 0
			UNION ALL
			SELECT 'HKDataTypeIdentifierHeartbeatSeries', 'heartbeatSeries', NULL::text, count(*)::bigint, 0::bigint,
			       (extract(epoch FROM min(start_ts)) * 1000)::bigint,
			       (extract(epoch FROM max(start_ts)) * 1000)::bigint
			FROM heartbeat_series
			WHERE user_id = $1
			HAVING count(*) > 0
			UNION ALL
			SELECT 'HKDataTypeIdentifierElectrocardiogram', 'ecg', NULL::text, count(*)::bigint, 0::bigint,
			       (extract(epoch FROM min(start_ts)) * 1000)::bigint,
			       (extract(epoch FROM max(start_ts)) * 1000)::bigint
			FROM ecg_samples
			WHERE user_id = $1
			HAVING count(*) > 0
			UNION ALL
			SELECT 'HKDataTypeIdentifierStateOfMind', 'stateOfMind', NULL::text, count(*)::bigint, 0::bigint,
			       (extract(epoch FROM min(start_ts)) * 1000)::bigint,
			       (extract(epoch FROM max(start_ts)) * 1000)::bigint
			FROM state_of_mind
			WHERE user_id = $1
			HAVING count(*) > 0
			UNION ALL
			SELECT 'HKMedicationDoseEventTypeIdentifierMedicationDoseEvent', 'medicationDose', NULL::text, count(*)::bigint, 0::bigint,
			       (extract(epoch FROM min(start_ts)) * 1000)::bigint,
			       (extract(epoch FROM max(start_ts)) * 1000)::bigint
			FROM medication_dose_events
			WHERE user_id = $1
			HAVING count(*) > 0
			UNION ALL
			SELECT 'HKActivitySummaryTypeIdentifier', 'activitySummary', NULL::text, count(*)::bigint, 0::bigint,
			       (extract(epoch FROM (min(date)::timestamp AT TIME ZONE 'UTC')) * 1000)::bigint,
			       (extract(epoch FROM (max(date)::timestamp AT TIME ZONE 'UTC')) * 1000)::bigint
			FROM activity_summaries
			WHERE user_id = $1
			HAVING count(*) > 0
			UNION ALL
			SELECT st.identifier, st.kind, st.unit, 0::bigint, count(*)::bigint,
			       (extract(epoch FROM min(a.bucket_start)) * 1000)::bigint,
			       (extract(epoch FROM max(a.bucket_start)) * 1000)::bigint
			FROM aggregate_samples a
			JOIN aggregate_series s USING (series_id)
			JOIN sample_types st USING (type_id)
			WHERE a.user_id = $1
			GROUP BY st.identifier, st.kind, st.unit
		)
		SELECT identifier, kind, unit,
		       sum(raw_rows + aggregate_rows)::bigint AS rows,
		       sum(raw_rows)::bigint AS raw_rows,
		       sum(aggregate_rows)::bigint AS aggregate_rows,
		       min(earliest) AS earliest,
		       max(latest) AS latest
		FROM per_table
		GROUP BY identifier, kind, unit
		ORDER BY identifier`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]CatalogType, 0)
	for rows.Next() {
		var ct CatalogType
		if err := rows.Scan(
			&ct.Identifier,
			&ct.Kind,
			&ct.Unit,
			&ct.Rows,
			&ct.RawRows,
			&ct.AggregateRows,
			&ct.Earliest,
			&ct.Latest,
		); err != nil {
			return nil, err
		}
		out = append(out, ct)
	}
	return out, rows.Err()
}

func (st *Store) LatestMetrics(ctx context.Context, userID string, types []string) ([]LatestMetric, error) {
	rows, err := st.pool.Query(ctx, `
		SELECT DISTINCT ON (st.identifier)
		       st.identifier, st.unit, q.value::float8,
		       (extract(epoch FROM q.start_ts) * 1000)::bigint AS t
		FROM quantity_samples q
		JOIN sample_types st ON st.type_id = q.type_id
		WHERE q.user_id = $1
		  AND st.identifier = ANY($2::text[])
		ORDER BY st.identifier, q.start_ts DESC`, userID, types)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byType := make(map[string]LatestMetric, len(types))
	for rows.Next() {
		var metric LatestMetric
		if err := rows.Scan(&metric.Identifier, &metric.Unit, &metric.Value, &metric.Timestamp); err != nil {
			return nil, err
		}
		byType[metric.Identifier] = metric
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]LatestMetric, 0, len(byType))
	for _, identifier := range types {
		if metric, ok := byType[identifier]; ok {
			out = append(out, metric)
		}
	}
	return out, nil
}

func (st *Store) DailyMetrics(ctx context.Context, userID string, f DailyFilters) ([]DailyMetric, error) {
	loc, err := st.UserLocation(ctx, userID)
	if err != nil {
		return nil, err
	}
	startDay, endDay, err := localDayRange(f.Start, f.End, loc)
	if err != nil {
		return nil, err
	}

	// Ordered by the position of each identifier in the request, then by
	// day: the same order the response nests, so LIMIT/OFFSET pages are
	// contiguous slices of what an unpaged answer would be.
	rows, err := st.pool.Query(ctx, `
		SELECT md.identifier, st.unit, md.day::text, md.value::float8
		FROM metric_daily md
		JOIN sample_types st ON st.type_id = md.type_id
		WHERE md.day >= $1::date
		  AND md.day < $2::date
		  AND md.user_id = $3
		  AND md.identifier = ANY($4::text[])
		ORDER BY array_position($4::text[], md.identifier), md.day
		LIMIT $5 OFFSET $6`, startDay, endDay, userID, f.Types, nullableLimit(f.Limit), max(f.Offset, 0))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byType := make(map[string]*DailyMetric, len(f.Types))
	order := make([]string, 0, len(f.Types))
	for rows.Next() {
		var (
			identifier string
			unit       *string
			day        string
			value      *float64
		)
		if err := rows.Scan(&identifier, &unit, &day, &value); err != nil {
			return nil, err
		}
		metric, ok := byType[identifier]
		if !ok {
			metric = &DailyMetric{Identifier: identifier, Unit: unit}
			byType[identifier] = metric
			order = append(order, identifier)
		}
		metric.Days = append(metric.Days, DailyPoint{Date: day, Value: value})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]DailyMetric, 0, len(order))
	for _, identifier := range f.Types {
		if metric, ok := byType[identifier]; ok {
			out = append(out, *metric)
		}
	}
	return out, nil
}

func (st *Store) ActivitySummary(ctx context.Context, userID string, start, end time.Time) ([]ActivityDay, error) {
	loc, err := st.UserLocation(ctx, userID)
	if err != nil {
		return nil, err
	}
	startDay, endDay, err := localDayRange(start, end, loc)
	if err != nil {
		return nil, err
	}

	rows, err := st.pool.Query(ctx, `
		SELECT date::text, move_kcal::float8, move_goal_kcal::float8,
		       exercise_min::float8, exercise_goal_min::float8,
		       stand_hours::float8, stand_goal_hours::float8, move_mode,
		       move_time_min::float8, move_time_goal_min::float8
		FROM activity_summaries
		WHERE user_id = $1
		  AND date >= $2::date AND date < $3::date
		ORDER BY date`, userID, startDay, endDay)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]ActivityDay, 0)
	for rows.Next() {
		var day ActivityDay
		if err := rows.Scan(
			&day.Date,
			&day.MoveKcal,
			&day.MoveGoalKcal,
			&day.ExerciseMin,
			&day.ExerciseGoalMin,
			&day.StandHours,
			&day.StandGoalHours,
			&day.MoveMode,
			&day.MoveTimeMin,
			&day.MoveTimeGoalMin,
		); err != nil {
			return nil, err
		}
		out = append(out, day)
	}
	return out, rows.Err()
}

// workoutSummarySQL is the one query behind both /v1/workouts and the
// workouts export. LIMIT takes NULL for "no limit" (nullableLimit), which is
// what the export passes.
const workoutSummarySQL = `
	SELECT w.uuid::text, w.activity_type, w.start_ts, w.end_ts,
	       w.duration_s::float8, w.distance_m::float8, w.energy_kcal::float8,
	       COALESCE(route_points.route_point_count, 0) > 0 AS has_route,
	       COALESCE(route_points.route_point_count, 0) AS route_point_count,
	       CASE
	           WHEN w.metadata ? 'HKIndoorWorkout' THEN
	               CASE lower(w.metadata->>'HKIndoorWorkout')
	                   WHEN '1' THEN TRUE
	                   WHEN 'true' THEN TRUE
	                   WHEN '0' THEN FALSE
	                   WHEN 'false' THEN FALSE
	                   ELSE NULL
	               END
	           ELSE NULL
	       END AS is_indoor,
	       CASE
	           WHEN jsonb_typeof(w.metadata->'HKElevationAscended') = 'number'
	                OR (w.metadata ? 'HKElevationAscended' AND (w.metadata->>'HKElevationAscended') ~ '^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$')
	           THEN (w.metadata->>'HKElevationAscended')::float8
	           ELSE NULL
	       END AS elevation_ascended_m,
	       COALESCE(metric_streams.available_metrics, ARRAY[]::text[]) AS available_metrics
	FROM workouts w
	LEFT JOIN LATERAL (
	  SELECT count(*)::bigint AS route_point_count
	  FROM workout_route_points r
	  WHERE r.workout_uuid = w.uuid
	    AND r.user_id = w.user_id
	) route_points ON TRUE
	LEFT JOIN LATERAL (
	  SELECT array_agg(DISTINCT st.identifier ORDER BY st.identifier) AS available_metrics
	  FROM workout_series_points wsp
	  JOIN sample_types st ON st.type_id = wsp.type_id
	  WHERE wsp.workout_uuid = w.uuid
	    AND wsp.user_id = w.user_id
	) metric_streams ON TRUE
	WHERE w.user_id = $1
	  AND ($2::timestamptz IS NULL OR w.start_ts >= $2)
	  AND ($3::timestamptz IS NULL OR w.start_ts < $3)
	  AND ($4::text = '' OR w.activity_type = $4)
	ORDER BY w.start_ts DESC, w.uuid DESC
	LIMIT $5::bigint OFFSET $6`

// StreamWorkouts calls fn once per workout matching filters, newest first,
// never holding more than one row. A Limit of zero or less means every
// match — what the export passes; Workouts passes the endpoint's page size.
// fn's error stops the scan and comes back unchanged.
func (st *Store) StreamWorkouts(ctx context.Context, userID string, filters WorkoutFilters, fn func(WorkoutSummary) error) error {
	rows, err := st.pool.Query(ctx, workoutSummarySQL,
		userID,
		filters.Start,
		filters.End,
		filters.ActivityType,
		nullableLimit(filters.Limit),
		filters.Offset,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		summary, err := scanWorkoutSummary(rows)
		if err != nil {
			return err
		}
		if err := fn(summary); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (st *Store) Workouts(ctx context.Context, userID string, filters WorkoutFilters) ([]WorkoutSummary, error) {
	out := make([]WorkoutSummary, 0)
	if err := st.StreamWorkouts(ctx, userID, filters, func(summary WorkoutSummary) error {
		out = append(out, summary)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// nullableLimit renders a row limit for SQL: zero or less becomes NULL,
// which Postgres reads as "no limit" — how the export asks for a whole
// range without a second copy of the query.
func nullableLimit(limit int) *int {
	if limit <= 0 {
		return nil
	}
	return &limit
}

func (st *Store) Workout(ctx context.Context, userID, uuid string) (*WorkoutDetail, error) {
	row := st.pool.QueryRow(ctx, `
		SELECT w.uuid::text, w.activity_type, w.start_ts, w.end_ts,
		       w.duration_s::float8, w.distance_m::float8, w.energy_kcal::float8,
		       COALESCE(route_points.route_point_count, 0) > 0 AS has_route,
		       COALESCE(route_points.route_point_count, 0) AS route_point_count,
		       CASE
		           WHEN w.metadata ? 'HKIndoorWorkout' THEN
		               CASE lower(w.metadata->>'HKIndoorWorkout')
		                   WHEN '1' THEN TRUE
		                   WHEN 'true' THEN TRUE
		                   WHEN '0' THEN FALSE
		                   WHEN 'false' THEN FALSE
		                   ELSE NULL
		               END
		           ELSE NULL
		       END AS is_indoor,
		       CASE
		           WHEN jsonb_typeof(w.metadata->'HKElevationAscended') = 'number'
		                OR (w.metadata ? 'HKElevationAscended' AND (w.metadata->>'HKElevationAscended') ~ '^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$')
		           THEN (w.metadata->>'HKElevationAscended')::float8
		           ELSE NULL
		       END AS elevation_ascended_m,
		       COALESCE(metric_streams.available_metrics, ARRAY[]::text[]) AS available_metrics,
		       w.stats_detail, w.events, w.activities
		FROM workouts w
		LEFT JOIN LATERAL (
		  SELECT count(*)::bigint AS route_point_count
		  FROM workout_route_points r
		  WHERE r.workout_uuid = w.uuid
		    AND r.user_id = w.user_id
		) route_points ON TRUE
		LEFT JOIN LATERAL (
		  SELECT array_agg(DISTINCT st.identifier ORDER BY st.identifier) AS available_metrics
		  FROM workout_series_points wsp
		  JOIN sample_types st ON st.type_id = wsp.type_id
		  WHERE wsp.workout_uuid = w.uuid
		    AND wsp.user_id = w.user_id
		) metric_streams ON TRUE
		WHERE w.user_id = $1
		  AND w.uuid = $2`, userID, uuid)

	var (
		summary         WorkoutSummary
		statsDetailJSON []byte
		eventsJSON      []byte
		activitiesJSON  []byte
	)
	if err := scanWorkoutSummaryRow(row, &summary, &statsDetailJSON, &eventsJSON, &activitiesJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	detail := &WorkoutDetail{WorkoutSummary: summary}
	if len(statsDetailJSON) > 0 {
		if err := json.Unmarshal(statsDetailJSON, &detail.StatisticsDetail); err != nil {
			return nil, fmt.Errorf("decode stats_detail: %w", err)
		}
	}
	if len(eventsJSON) > 0 {
		if err := json.Unmarshal(eventsJSON, &detail.Events); err != nil {
			return nil, fmt.Errorf("decode events: %w", err)
		}
	}
	if len(activitiesJSON) > 0 {
		if err := json.Unmarshal(activitiesJSON, &detail.Activities); err != nil {
			return nil, fmt.Errorf("decode activities: %w", err)
		}
	}
	return detail, nil
}

func scanWorkoutSummary(scanner interface {
	Scan(dest ...any) error
}) (WorkoutSummary, error) {
	var (
		summary    WorkoutSummary
		start, end time.Time
	)
	if err := scanner.Scan(
		&summary.UUID,
		&summary.ActivityType,
		&start,
		&end,
		&summary.DurationS,
		&summary.DistanceM,
		&summary.EnergyKcal,
		&summary.HasRoute,
		&summary.RoutePointCount,
		&summary.IsIndoor,
		&summary.ElevationAscendedM,
		&summary.AvailableMetrics,
	); err != nil {
		return WorkoutSummary{}, err
	}
	summary.Start = start.UTC().UnixMilli()
	summary.End = end.UTC().UnixMilli()
	return summary, nil
}

func scanWorkoutSummaryRow(
	row pgx.Row,
	summary *WorkoutSummary,
	statsDetailJSON, eventsJSON, activitiesJSON *[]byte,
) error {
	var start, end time.Time
	if err := row.Scan(
		&summary.UUID,
		&summary.ActivityType,
		&start,
		&end,
		&summary.DurationS,
		&summary.DistanceM,
		&summary.EnergyKcal,
		&summary.HasRoute,
		&summary.RoutePointCount,
		&summary.IsIndoor,
		&summary.ElevationAscendedM,
		&summary.AvailableMetrics,
		statsDetailJSON,
		eventsJSON,
		activitiesJSON,
	); err != nil {
		return err
	}
	summary.Start = start.UTC().UnixMilli()
	summary.End = end.UTC().UnixMilli()
	return nil
}

// requestError is a store failure the request caused — an identifier the
// database has never seen, a range over a cap — and is answered with 400
// rather than logged as a 500.
type requestError struct{ msg string }

func (e *requestError) Error() string { return e.msg }

func badRequestf(format string, args ...any) error {
	return &requestError{msg: fmt.Sprintf(format, args...)}
}

// localDayRange returns the inclusive first and exclusive last calendar day
// (YYYY-MM-DD, in loc) touched by the instant range [start, end).
func localDayRange(start, end time.Time, loc *time.Location) (string, string, error) {
	first, afterLast, err := localDayBounds(start, end, loc)
	if err != nil {
		return "", "", err
	}
	return first.Format("2006-01-02"), afterLast.Format("2006-01-02"), nil
}

// localDayBounds returns the instants of local midnight (in loc) that begin
// the first calendar day touched by [start, end) and the day after the last
// one — the same days localDayRange names, as timestamps a query can
// compare against.
func localDayBounds(start, end time.Time, loc *time.Location) (time.Time, time.Time, error) {
	if !end.After(start) {
		return time.Time{}, time.Time{}, fmt.Errorf("end must be after start")
	}
	if loc == nil {
		loc = time.UTC
	}

	startLocal := start.In(loc)
	first := time.Date(startLocal.Year(), startLocal.Month(), startLocal.Day(), 0, 0, 0, 0, loc)
	lastTouchedLocal := end.Add(-time.Nanosecond).In(loc)
	afterLast := time.Date(
		lastTouchedLocal.Year(),
		lastTouchedLocal.Month(),
		lastTouchedLocal.Day(),
		0, 0, 0, 0,
		loc,
	).AddDate(0, 0, 1)
	return first, afterLast, nil
}

// calendarDays counts the calendar days from the local midnight first up to
// but excluding the local midnight afterLast. The arithmetic runs on the
// dates in UTC so a DST change inside the span cannot skew it.
func calendarDays(first, afterLast time.Time) int {
	f := time.Date(first.Year(), first.Month(), first.Day(), 0, 0, 0, 0, time.UTC)
	a := time.Date(afterLast.Year(), afterLast.Month(), afterLast.Day(), 0, 0, 0, 0, time.UTC)
	return int(a.Sub(f).Hours() / 24)
}
