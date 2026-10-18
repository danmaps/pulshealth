package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationStore(t *testing.T) (*Store, context.Context, func()) {
	t.Helper()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		skipIntegration(t, "DATABASE_URL is unset")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		cancel()
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		cancel()
		t.Fatalf("pool.Ping: %v", err)
	}

	cleanup := func() {
		pool.Close()
		cancel()
	}
	loc, err := losAngelesLocation()
	if err != nil {
		cleanup()
		t.Fatalf("losAngelesLocation: %v", err)
	}
	return NewStore(pool, loc), ctx, cleanup
}

// The day-range fixtures below were written against America/Los_Angeles. The
// zone is a per-deployment setting now (PULS_TIME_ZONE, loaded once in
// main.go and handed to NewStore), so the tests pass it explicitly.
func losAngelesLocation() (*time.Location, error) {
	return time.LoadLocation("America/Los_Angeles")
}

// writeIntegrationStore is integrationStore plus a second pool that may
// write fixture rows. The store under test reads through DATABASE_URL —
// point that at the read-only api_reader role to exercise its grants — and
// fixtures go through ADMIN_DATABASE_URL (the superuser), which falls back
// to DATABASE_URL so a suite run entirely as the superuser keeps working.
func writeIntegrationStore(t *testing.T) (*Store, *pgxpool.Pool, context.Context, func()) {
	t.Helper()

	if os.Getenv("PULS_API_WRITE_INTEGRATION_TESTS") != "1" {
		skipIntegration(t, "PULS_API_WRITE_INTEGRATION_TESTS is not set to 1, so the tests that write fixture rows do not run")
	}
	store, ctx, cleanup := integrationStore(t)

	adminURL := os.Getenv("ADMIN_DATABASE_URL")
	if adminURL == "" {
		adminURL = os.Getenv("DATABASE_URL")
	}
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		cleanup()
		t.Fatalf("pgxpool.New(admin): %v", err)
	}
	if err := admin.Ping(ctx); err != nil {
		admin.Close()
		cleanup()
		t.Fatalf("admin.Ping: %v", err)
	}
	return store, admin, ctx, func() {
		admin.Close()
		cleanup()
	}
}

// ensureSampleType returns the type_id of identifier, inserting it with the
// given kind and unit when the database has never seen it; the returned
// func removes it again only if this call created it.
func ensureSampleType(t *testing.T, ctx context.Context, admin *pgxpool.Pool, identifier, kind string, unit *string) (int16, func()) {
	t.Helper()
	var typeID int16
	err := admin.QueryRow(ctx, `SELECT type_id FROM sample_types WHERE identifier = $1`, identifier).Scan(&typeID)
	if err == nil {
		return typeID, func() {}
	}
	if err := admin.QueryRow(ctx, `
		INSERT INTO sample_types (identifier, kind, unit) VALUES ($1, $2, $3)
		RETURNING type_id`, identifier, kind, unit).Scan(&typeID); err != nil {
		t.Fatalf("insert sample_types %s: %v", identifier, err)
	}
	return typeID, func() {
		_, _ = admin.Exec(context.Background(), `DELETE FROM sample_types WHERE type_id = $1`, typeID)
	}
}

// sleepValue looks a sleep stage's integer value up by its HealthKit enum
// name, so the fixtures never hardcode category integers.
func sleepValue(t *testing.T, ctx context.Context, admin *pgxpool.Pool, enumName string) int16 {
	t.Helper()
	var value int16
	if err := admin.QueryRow(ctx, `
		SELECT value FROM category_labels WHERE type_identifier = $1 AND enum_name = $2`,
		sleepTypeIdentifier, enumName).Scan(&value); err != nil {
		t.Fatalf("category_labels has no %s: %v", enumName, err)
	}
	return value
}

func insertSource(t *testing.T, ctx context.Context, admin *pgxpool.Pool, name string) (int16, func()) {
	t.Helper()
	var id int16
	if err := admin.QueryRow(ctx, `
		INSERT INTO sources (name, bundle_id, version) VALUES ($1, 'test', '1')
		RETURNING source_id`, name).Scan(&id); err != nil {
		t.Fatalf("insert source %s: %v", name, err)
	}
	return id, func() {
		_, _ = admin.Exec(context.Background(), `DELETE FROM sources WHERE source_id = $1`, id)
	}
}

func fixtureUUID(prefix string, suffix int64, n int) string {
	return fmt.Sprintf("%s-%04x-4000-8000-%012d", prefix, n, suffix%1_000_000_000_000)
}

// fixtureDay puts a run's fixtures on their own far-future day, so a second
// run — or rows someone seeded by hand — is unlikely to land on the same
// dates. Each caller passes a different year, which keeps the tests apart
// from one another as well. Days are drawn from a summer window because
// America/Los_Angeles has no DST transition in it: a night that spans one
// would be an hour shorter than the wall clock says and break the minute
// arithmetic the sleep fixtures assert on.
func fixtureDay(year int, loc *time.Location, suffix int64) time.Time {
	return time.Date(year, 6, 1, 0, 0, 0, 0, loc).AddDate(0, 0, int(suffix%80))
}

func TestLocalDayRangeUsesLosAngelesDates(t *testing.T) {
	loc, err := losAngelesLocation()
	if err != nil {
		t.Fatalf("losAngelesLocation: %v", err)
	}

	start := time.Date(2026, 7, 1, 7, 30, 0, 0, time.UTC)
	end := time.Date(2026, 7, 2, 7, 30, 0, 0, time.UTC)
	gotStart, gotEnd, err := localDayRange(start, end, loc)
	if err != nil {
		t.Fatalf("localDayRange: %v", err)
	}
	wantStart := start.In(loc).Format("2006-01-02")
	wantEnd := "2026-07-03"
	if gotStart != wantStart || gotEnd != wantEnd {
		t.Fatalf("localDayRange = %q,%q want %q,%q", gotStart, gotEnd, wantStart, wantEnd)
	}
}

func TestLocalDayRangeCrossesLosAngelesDayBoundary(t *testing.T) {
	loc, err := losAngelesLocation()
	if err != nil {
		t.Fatalf("losAngelesLocation: %v", err)
	}
	start := time.Date(2026, 7, 2, 6, 30, 0, 0, time.UTC)
	end := time.Date(2026, 7, 2, 8, 30, 0, 0, time.UTC)
	gotStart, gotEnd, err := localDayRange(start, end, loc)
	if err != nil {
		t.Fatalf("localDayRange: %v", err)
	}
	wantStart := "2026-07-01"
	wantEnd := "2026-07-03"
	if gotStart != wantStart || gotEnd != wantEnd {
		t.Fatalf("localDayRange = %q,%q want %q,%q", gotStart, gotEnd, wantStart, wantEnd)
	}
}

func TestLocalDayRangeTouchedLosAngelesDays(t *testing.T) {
	loc, err := losAngelesLocation()
	if err != nil {
		t.Fatalf("losAngelesLocation: %v", err)
	}

	tests := []struct {
		name      string
		start     time.Time
		end       time.Time
		wantStart string
		wantEnd   string
	}{
		{
			name:      "single touched day exact midnight bounds",
			start:     time.Date(2026, 7, 2, 0, 0, 0, 0, loc),
			end:       time.Date(2026, 7, 3, 0, 0, 0, 0, loc),
			wantStart: "2026-07-02",
			wantEnd:   "2026-07-03",
		},
		{
			name:      "crosses into next day after midnight",
			start:     time.Date(2026, 7, 2, 23, 30, 0, 0, loc),
			end:       time.Date(2026, 7, 3, 1, 30, 0, 0, loc),
			wantStart: "2026-07-02",
			wantEnd:   "2026-07-04",
		},
		{
			name:      "end exactly at midnight excludes new day",
			start:     time.Date(2026, 7, 2, 23, 30, 0, 0, loc),
			end:       time.Date(2026, 7, 3, 0, 0, 0, 0, loc),
			wantStart: "2026-07-02",
			wantEnd:   "2026-07-03",
		},
		{
			name:      "small positive range across spring forward still counts both days",
			start:     time.Date(2026, 3, 7, 23, 30, 0, 0, loc),
			end:       time.Date(2026, 3, 8, 1, 30, 0, 0, loc),
			wantStart: "2026-03-07",
			wantEnd:   "2026-03-09",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd, err := localDayRange(tt.start, tt.end, loc)
			if err != nil {
				t.Fatalf("localDayRange: %v", err)
			}
			if gotStart != tt.wantStart || gotEnd != tt.wantEnd {
				t.Fatalf("localDayRange = %q,%q want %q,%q", gotStart, gotEnd, tt.wantStart, tt.wantEnd)
			}
		})
	}
}

func TestLocalDayRangeRejectsEmptyOrNegativeRange(t *testing.T) {
	start := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	for _, end := range []time.Time{start, start.Add(-time.Nanosecond)} {
		if _, _, err := localDayRange(start, end, time.UTC); err == nil {
			t.Fatalf("expected error for start=%s end=%s", start, end)
		}
	}
}

func TestIntegrationCatalogTypesEmptyOrLive(t *testing.T) {
	store, ctx, cleanup := integrationStore(t)
	defer cleanup()

	types, err := store.CatalogTypes(ctx, defaultUserID)
	if err != nil {
		t.Fatalf("CatalogTypes: %v", err)
	}
	for i, typ := range types {
		if typ.Identifier == "" {
			t.Fatalf("types[%d] has empty identifier: %#v", i, typ)
		}
	}
}

func TestIntegrationUnknownLatestMetricIsEmpty(t *testing.T) {
	store, ctx, cleanup := integrationStore(t)
	defer cleanup()

	metrics, err := store.LatestMetrics(ctx, defaultUserID, []string{"HKQuantityTypeIdentifierDefinitelyMissingForIntegrationTest"})
	if err != nil {
		t.Fatalf("LatestMetrics: %v", err)
	}
	if len(metrics) != 0 {
		t.Fatalf("metrics len = %d, want 0", len(metrics))
	}
	if metrics == nil {
		t.Fatal("metrics is nil, want empty non-nil slice")
	}
}

func TestIntegrationActivitySummaryEmptyRangeIsNonNil(t *testing.T) {
	store, ctx, cleanup := integrationStore(t)
	defer cleanup()

	start := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	days, err := store.ActivitySummary(ctx, defaultUserID, start, end)
	if err != nil {
		t.Fatalf("ActivitySummary: %v", err)
	}
	if len(days) != 0 {
		t.Fatalf("days len = %d, want 0", len(days))
	}
	if days == nil {
		t.Fatal("days is nil, want empty non-nil slice")
	}
}

func TestIntegrationActivitySummaryUsesTouchedLocalDays(t *testing.T) {
	store, admin, ctx, cleanup := writeIntegrationStore(t)
	defer cleanup()

	loc, err := losAngelesLocation()
	if err != nil {
		t.Fatalf("losAngelesLocation: %v", err)
	}
	firstDay := time.Date(2099, 9, 17, 0, 0, 0, 0, loc)
	secondDay := firstDay.AddDate(0, 0, 1)
	defer func() {
		_, _ = admin.Exec(
			context.Background(),
			`DELETE FROM activity_summaries WHERE user_id = $1 AND date IN ($2::date, $3::date)`,
			defaultUserID,
			firstDay.Format("2006-01-02"),
			secondDay.Format("2006-01-02"),
		)
	}()

	if _, err := admin.Exec(ctx, `
		INSERT INTO activity_summaries (user_id, date, move_kcal)
		VALUES ($1, $2::date, 100), ($1, $3::date, 200)`,
		defaultUserID,
		firstDay.Format("2006-01-02"),
		secondDay.Format("2006-01-02"),
	); err != nil {
		t.Fatalf("insert activity summaries: %v", err)
	}

	start := time.Date(2099, 9, 17, 23, 30, 0, 0, loc)
	end := time.Date(2099, 9, 18, 1, 30, 0, 0, loc)
	days, err := store.ActivitySummary(ctx, defaultUserID, start, end)
	if err != nil {
		t.Fatalf("ActivitySummary: %v", err)
	}
	if len(days) != 2 {
		t.Fatalf("days = %#v, want both touched local days", days)
	}
	if days[0].Date != "2099-09-17" || days[1].Date != "2099-09-18" {
		t.Fatalf("day dates = %q,%q, want 2099-09-17,2099-09-18", days[0].Date, days[1].Date)
	}
}

func TestIntegrationWorkoutsEmptyFilterIsNonNil(t *testing.T) {
	store, ctx, cleanup := integrationStore(t)
	defer cleanup()

	start := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	workouts, err := store.Workouts(ctx, defaultUserID, WorkoutFilters{
		Start:  &start,
		End:    &end,
		Limit:  10,
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("Workouts: %v", err)
	}
	if len(workouts) != 0 {
		t.Fatalf("workouts len = %d, want 0", len(workouts))
	}
	if workouts == nil {
		t.Fatal("workouts is nil, want empty non-nil slice")
	}
}

func TestIntegrationDailyMetricsFixture(t *testing.T) {
	store, admin, ctx, cleanup := writeIntegrationStore(t)
	defer cleanup()

	suffix := time.Now().UTC().UnixNano()
	identifier := fmt.Sprintf("HKQuantityTypeIdentifierCodexDailyMetrics%d", suffix)

	var typeID int16
	if err := admin.QueryRow(ctx, `
		INSERT INTO sample_types (identifier, kind, unit)
		VALUES ($1, 'quantity', 'count')
		RETURNING type_id`, identifier).Scan(&typeID); err != nil {
		t.Fatalf("insert sample_types: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DELETE FROM aggregate_samples WHERE series_id IN (SELECT series_id FROM aggregate_series WHERE type_id = $1)`, typeID)
		_, _ = admin.Exec(context.Background(), `DELETE FROM aggregate_series WHERE type_id = $1`, typeID)
		_, _ = admin.Exec(context.Background(), `DELETE FROM sample_types WHERE type_id = $1`, typeID)
	}()

	var seriesID int16
	if err := admin.QueryRow(ctx, `
		INSERT INTO aggregate_series (type_id, agg_func, interval_value, interval_unit, device_filter, unit)
		VALUES ($1, 'sum', 1, 'day', 'all', 'count')
		RETURNING series_id`, typeID).Scan(&seriesID); err != nil {
		t.Fatalf("insert aggregate_series: %v", err)
	}

	loc, err := losAngelesLocation()
	if err != nil {
		t.Fatalf("losAngelesLocation: %v", err)
	}
	dayStart := time.Date(2099, 7, 3, 0, 0, 0, 0, loc)
	dayEnd := dayStart.Add(24 * time.Hour)
	if _, err := admin.Exec(ctx, `
		INSERT INTO aggregate_samples (series_id, bucket_start, bucket_end, value, user_id)
		VALUES ($1, $2, $3, $4, $5)`,
		seriesID, dayStart, dayEnd, 123.0, defaultUserID,
	); err != nil {
		t.Fatalf("insert aggregate_samples: %v", err)
	}

	rangeStart := time.Date(2099, 7, 3, 0, 0, 0, 0, loc)
	rangeEnd := time.Date(2099, 7, 4, 0, 0, 0, 0, loc)
	metrics, err := store.DailyMetrics(ctx, defaultUserID, DailyFilters{Types: []string{identifier}, Start: rangeStart, End: rangeEnd})
	if err != nil {
		t.Fatalf("DailyMetrics: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("metrics len = %d, want 1", len(metrics))
	}
	if metrics[0].Identifier != identifier {
		t.Fatalf("identifier = %q, want %q", metrics[0].Identifier, identifier)
	}
	if len(metrics[0].Days) != 1 {
		t.Fatalf("days len = %d, want 1", len(metrics[0].Days))
	}
	if metrics[0].Days[0].Value == nil || *metrics[0].Days[0].Value != 123 {
		t.Fatalf("value = %v, want 123", metrics[0].Days[0].Value)
	}

	catalog, err := store.CatalogTypes(ctx, defaultUserID)
	if err != nil {
		t.Fatalf("CatalogTypes: %v", err)
	}
	for _, typ := range catalog {
		if typ.Identifier != identifier {
			continue
		}
		if typ.Rows != 1 || typ.RawRows != 0 || typ.AggregateRows != 1 {
			t.Fatalf("aggregate-only catalog counts = rows:%d raw:%d aggregate:%d, want 1/0/1",
				typ.Rows, typ.RawRows, typ.AggregateRows)
		}
		return
	}
	t.Fatalf("aggregate-only type %q missing from catalog", identifier)
}

func TestIntegrationWorkoutFixture(t *testing.T) {
	store, admin, ctx, cleanup := writeIntegrationStore(t)
	defer cleanup()

	suffix := time.Now().UTC().UnixNano()
	identifier := fmt.Sprintf("HKQuantityTypeIdentifierCodexWorkout%d", suffix)
	workoutUUID := fmt.Sprintf("aaaaaaaa-aaaa-4aaa-8aaa-%012d", suffix%1_000_000_000_000)
	workoutUUID2 := fmt.Sprintf("bbbbbbbb-bbbb-4bbb-8bbb-%012d", suffix%1_000_000_000_000)
	start := time.Date(2099, 8, 1, 10, 0, 0, 0, time.UTC)
	end := start.Add(45 * time.Minute)

	var typeID int16
	if err := admin.QueryRow(ctx, `
		INSERT INTO sample_types (identifier, kind, unit)
		VALUES ($1, 'quantity', 'count/min')
		RETURNING type_id`, identifier).Scan(&typeID); err != nil {
		t.Fatalf("insert sample_types: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DELETE FROM workout_route_points WHERE workout_uuid IN ($1, $2)`, workoutUUID, workoutUUID2)
		_, _ = admin.Exec(context.Background(), `DELETE FROM workout_series_points WHERE workout_uuid IN ($1, $2)`, workoutUUID, workoutUUID2)
		_, _ = admin.Exec(context.Background(), `DELETE FROM workouts WHERE uuid IN ($1, $2)`, workoutUUID, workoutUUID2)
		_, _ = admin.Exec(context.Background(), `DELETE FROM sample_types WHERE type_id = $1`, typeID)
	}()

	if _, err := admin.Exec(ctx, `
		INSERT INTO workouts (uuid, activity_type, start_ts, end_ts, duration_s, energy_kcal, distance_m, user_id, metadata, stats_detail, events, activities)
		VALUES ($1, 'HKWorkoutActivityTypeRunning', $2, $3, 2700, 500, 6000, $4,
		        '{"HKIndoorWorkout":0,"HKElevationAscended":42.5}'::jsonb,
		        '{"`+identifier+`":{"avg":68,"max":175}}'::jsonb,
		        '[{"kind":"lap","lap":1}]'::jsonb,
		        '[{"activityType":"HKWorkoutActivityTypeRunning"}]'::jsonb)`,
		workoutUUID, start, end, defaultUserID,
	); err != nil {
		t.Fatalf("insert workout: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		INSERT INTO workouts (uuid, activity_type, start_ts, end_ts, duration_s, user_id, metadata)
		VALUES ($1, 'HKWorkoutActivityTypeRunning', $2, $3, 2700, $4, '{"HKIndoorWorkout":1}'::jsonb)`,
		workoutUUID2, start, end, defaultUserID,
	); err != nil {
		t.Fatalf("insert tied-start workout: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		INSERT INTO workout_series_points (workout_uuid, type_id, ts, value, user_id)
		VALUES ($1, $2, $3, $4, $5)`,
		workoutUUID, typeID, start.Add(5*time.Minute), 68.0, defaultUserID,
	); err != nil {
		t.Fatalf("insert workout_series_points: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		INSERT INTO workout_route_points (workout_uuid, ts, lat, lon, user_id)
		VALUES ($1, $2, $3, $4, $5)`,
		workoutUUID, start.Add(2*time.Minute), 37.0, -122.0, defaultUserID,
	); err != nil {
		t.Fatalf("insert workout_route_points: %v", err)
	}

	detail, err := store.Workout(ctx, defaultUserID, workoutUUID)
	if err != nil {
		t.Fatalf("Workout: %v", err)
	}
	if detail == nil {
		t.Fatal("detail is nil")
	}
	if detail.UUID != workoutUUID {
		t.Fatalf("uuid = %q, want %q", detail.UUID, workoutUUID)
	}
	if !detail.HasRoute {
		t.Fatal("hasRoute = false, want true")
	}
	if detail.RoutePointCount != 1 {
		t.Fatalf("routePointCount = %d, want 1", detail.RoutePointCount)
	}
	if detail.IsIndoor == nil || *detail.IsIndoor {
		t.Fatalf("isIndoor = %v, want false", detail.IsIndoor)
	}
	if detail.ElevationAscendedM == nil || *detail.ElevationAscendedM != 42.5 {
		t.Fatalf("elevationAscendedM = %v, want 42.5", detail.ElevationAscendedM)
	}
	if len(detail.AvailableMetrics) != 1 || detail.AvailableMetrics[0] != identifier {
		t.Fatalf("availableMetrics = %#v, want [%q]", detail.AvailableMetrics, identifier)
	}
	stat, ok := detail.StatisticsDetail[identifier]
	if !ok || stat.Avg == nil || *stat.Avg != 68 {
		t.Fatalf("stats detail = %#v", detail.StatisticsDetail)
	}
	if len(detail.Events) != 1 || detail.Events[0]["kind"] != "lap" {
		t.Fatalf("events = %#v", detail.Events)
	}
	if len(detail.Activities) != 1 || detail.Activities[0]["activityType"] != "HKWorkoutActivityTypeRunning" {
		t.Fatalf("activities = %#v", detail.Activities)
	}

	rangeStart := start.Add(-time.Second)
	rangeEnd := end.Add(time.Second)
	firstPage, err := store.Workouts(ctx, defaultUserID, WorkoutFilters{Start: &rangeStart, End: &rangeEnd, Limit: 1})
	if err != nil {
		t.Fatalf("Workouts first page: %v", err)
	}
	secondPage, err := store.Workouts(ctx, defaultUserID, WorkoutFilters{Start: &rangeStart, End: &rangeEnd, Limit: 1, Offset: 1})
	if err != nil {
		t.Fatalf("Workouts second page: %v", err)
	}
	if len(firstPage) != 1 || len(secondPage) != 1 {
		t.Fatalf("page lengths = %d,%d, want 1,1", len(firstPage), len(secondPage))
	}
	if firstPage[0].UUID != workoutUUID2 || secondPage[0].UUID != workoutUUID {
		t.Fatalf("tied-start UUID order = %q,%q, want %q,%q", firstPage[0].UUID, secondPage[0].UUID, workoutUUID2, workoutUUID)
	}

	// The export path: a zero Limit is "no limit", so both tied workouts
	// come back from one scan in the same order the paged reads produced.
	var streamed []string
	if err := store.StreamWorkouts(ctx, defaultUserID, WorkoutFilters{Start: &rangeStart, End: &rangeEnd}, func(w WorkoutSummary) error {
		streamed = append(streamed, w.UUID)
		return nil
	}); err != nil {
		t.Fatalf("StreamWorkouts: %v", err)
	}
	if len(streamed) != 2 || streamed[0] != workoutUUID2 || streamed[1] != workoutUUID {
		t.Fatalf("streamed = %#v, want %q then %q", streamed, workoutUUID2, workoutUUID)
	}
}

// wantRequestError asserts err is a requestError (a 400 to the caller) whose
// message mentions each fragment.
func wantRequestError(t *testing.T, err error, fragments ...string) {
	t.Helper()
	var reqErr *requestError
	if !errors.As(err, &reqErr) {
		t.Fatalf("error = %v (%T), want a *requestError", err, err)
	}
	for _, fragment := range fragments {
		if !strings.Contains(reqErr.Error(), fragment) {
			t.Errorf("error %q does not mention %q", reqErr, fragment)
		}
	}
}

// insertSleepSample writes one HKCategoryTypeIdentifierSleepAnalysis row,
// looking its integer value up by HealthKit enum name.
func insertSleepSample(
	t *testing.T, ctx context.Context, admin *pgxpool.Pool,
	typeID, sourceID int16, uuid, enumName string, start, end time.Time,
) {
	t.Helper()
	if _, err := admin.Exec(ctx, `
		INSERT INTO category_samples (uuid, type_id, start_ts, end_ts, value, source_id, user_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		uuid, typeID, start, end, sleepValue(t, ctx, admin, enumName), sourceID, defaultUserID,
	); err != nil {
		t.Fatalf("insert sleep sample %s: %v", enumName, err)
	}
}

// A night recorded twice: an iPhone logs the in-bed window and a stage-less
// "asleep" interval while the Watch logs stages. It crosses local midnight,
// which is the case wake-up-day attribution has to get right, and a nap the
// next afternoon must come back as its own row on the same date.
func TestIntegrationSleepDailyAcrossMidnightFromTwoSources(t *testing.T) {
	store, admin, ctx, cleanup := writeIntegrationStore(t)
	defer cleanup()

	loc, err := losAngelesLocation()
	if err != nil {
		t.Fatalf("losAngelesLocation: %v", err)
	}
	suffix := time.Now().UTC().UnixNano()
	typeID, dropType := ensureSampleType(t, ctx, admin, sleepTypeIdentifier, "category", nil)
	defer dropType()
	watch, dropWatch := insertSource(t, ctx, admin, fmt.Sprintf("CodexWatch%d", suffix))
	defer dropWatch()
	phone, dropPhone := insertSource(t, ctx, admin, fmt.Sprintf("CodexPhone%d", suffix))
	defer dropPhone()

	// The night runs from the evening of eve into the morning of wake.
	eve := fixtureDay(2090, loc, suffix).Format("2006-01-02")
	wake := fixtureDay(2090, loc, suffix).AddDate(0, 0, 1).Format("2006-01-02")
	after := fixtureDay(2090, loc, suffix).AddDate(0, 0, 2).Format("2006-01-02")

	local := func(day, hhmm string) time.Time {
		ts, err := time.ParseInLocation("2006-01-02 15:04", day+" "+hhmm, loc)
		if err != nil {
			t.Fatalf("parse %s %s: %v", day, hhmm, err)
		}
		return ts
	}

	var uuids []string
	n := 0
	add := func(sourceID int16, enumName, startDay, startAt, endDay, endAt string) {
		n++
		uuid := fixtureUUID("cccccccc", suffix, n)
		uuids = append(uuids, uuid)
		insertSleepSample(t, ctx, admin, typeID, sourceID, uuid, enumName,
			local(startDay, startAt), local(endDay, endAt))
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DELETE FROM category_samples WHERE uuid = ANY($1)`, uuids)
	}()

	// iPhone: the scheduled in-bed window and 450 minutes of stage-less sleep.
	add(phone, "HKCategoryValueSleepAnalysisInBed", eve, "22:30", wake, "06:30")
	add(phone, "HKCategoryValueSleepAnalysisAsleepUnspecified", eve, "22:45", wake, "06:15")
	// Watch: 460 minutes of staged sleep plus ten minutes awake.
	add(watch, "HKCategoryValueSleepAnalysisAsleepCore", eve, "22:40", wake, "01:00")
	add(watch, "HKCategoryValueSleepAnalysisAsleepDeep", wake, "01:00", wake, "02:00")
	add(watch, "HKCategoryValueSleepAnalysisAsleepREM", wake, "02:00", wake, "03:00")
	add(watch, "HKCategoryValueSleepAnalysisAwake", wake, "03:00", wake, "03:10")
	add(watch, "HKCategoryValueSleepAnalysisAsleepCore", wake, "03:10", wake, "06:30")
	// An afternoon nap, more than three hours after waking.
	add(watch, "HKCategoryValueSleepAnalysisAsleepCore", wake, "14:00", wake, "15:00")

	nights, err := store.SleepDaily(ctx, defaultUserID, local(wake, "00:00"), local(after, "00:00"))
	if err != nil {
		t.Fatalf("SleepDaily: %v", err)
	}
	// Locate this run's two rows rather than assuming the database holds
	// nothing else on these dates.
	var night, nap *SleepNight
	nightAt, napAt := local(eve, "22:30").UnixMilli(), local(wake, "14:00").UnixMilli()
	for i := range nights {
		switch nights[i].Start {
		case nightAt:
			night = &nights[i]
		case napAt:
			nap = &nights[i]
		}
	}
	if night == nil || nap == nil {
		t.Fatalf("nights = %#v, want this run's night and nap", nights)
	}

	if night.Date != wake {
		t.Errorf("date = %q, want the wake-up day %s", night.Date, wake)
	}
	if night.Start != local(eve, "22:30").UnixMilli() || night.End != local(wake, "06:30").UnixMilli() {
		t.Errorf("night bounds = %d..%d", night.Start, night.End)
	}
	if night.InBedMinutes != 480 {
		t.Errorf("inBedMinutes = %v, want the phone's 480", night.InBedMinutes)
	}
	if night.AsleepMinutes != 460 {
		t.Errorf("asleepMinutes = %v, want the Watch's 460 (not 460+450)", night.AsleepMinutes)
	}
	if night.Stages != (SleepStages{Core: 340, Deep: 60, REM: 60, Awake: 10}) {
		t.Errorf("stages = %+v", night.Stages)
	}
	if night.Sources != 2 {
		t.Errorf("sources = %d, want 2", night.Sources)
	}

	// The nap is a row of its own, on the same date, three hours after
	// waking rather than part of the night.
	if nap.Date != wake || nap.AsleepMinutes != 60 || nap.Sources != 1 {
		t.Errorf("nap = %+v", *nap)
	}
	if nap.Start <= night.End {
		t.Errorf("nap starts at %d, before the night ended at %d", nap.Start, night.End)
	}

	// The evening the night began is not a wake-up day, so it has no row.
	before, err := store.SleepDaily(ctx, defaultUserID, local(eve, "00:00"), local(wake, "00:00"))
	if err != nil {
		t.Fatalf("SleepDaily (previous day): %v", err)
	}
	for _, candidate := range before {
		if candidate.Start == local(eve, "22:30").UnixMilli() {
			t.Errorf("the night was also reported on the day it started: %+v", candidate)
		}
	}

	if _, err := store.SleepDaily(ctx, defaultUserID, fixtureDay(2090, loc, suffix).AddDate(-2, 0, 0), local(after, "00:00")); err == nil {
		t.Error("a range over 366 days was accepted")
	} else {
		wantRequestError(t, err, "at most 366 days")
	}
}

func TestIntegrationSamplesQuantityAndCategory(t *testing.T) {
	store, admin, ctx, cleanup := writeIntegrationStore(t)
	defer cleanup()

	suffix := time.Now().UTC().UnixNano()
	unit := "count/min"
	quantityType := fmt.Sprintf("HKQuantityTypeIdentifierCodexSamples%d", suffix)
	quantityID, dropQuantity := ensureSampleType(t, ctx, admin, quantityType, "quantity", &unit)
	defer dropQuantity()
	sleepID, dropSleep := ensureSampleType(t, ctx, admin, sleepTypeIdentifier, "category", nil)
	defer dropSleep()
	workoutType := fmt.Sprintf("HKWorkoutTypeIdentifierCodex%d", suffix)
	_, dropWorkoutType := ensureSampleType(t, ctx, admin, workoutType, "workout", nil)
	defer dropWorkoutType()
	sourceID, dropSource := insertSource(t, ctx, admin, fmt.Sprintf("CodexSampleSource%d", suffix))
	defer dropSource()

	start := fixtureDay(2080, time.UTC, suffix).Add(10 * time.Hour)
	var quantityUUIDs []string
	for i := 0; i < 3; i++ {
		uuid := fixtureUUID("dddddddd", suffix, i)
		quantityUUIDs = append(quantityUUIDs, uuid)
		if _, err := admin.Exec(ctx, `
			INSERT INTO quantity_samples (uuid, type_id, start_ts, end_ts, value, source_id, user_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			uuid, quantityID, start.Add(time.Duration(i)*time.Minute), start.Add(time.Duration(i)*time.Minute),
			60.0+float64(i), sourceID, defaultUserID,
		); err != nil {
			t.Fatalf("insert quantity sample: %v", err)
		}
	}
	sleepUUID := fixtureUUID("eeeeeeee", suffix, 0)
	insertSleepSample(t, ctx, admin, sleepID, sourceID, sleepUUID,
		"HKCategoryValueSleepAnalysisAsleepDeep", start, start.Add(30*time.Minute))
	defer func() {
		bg := context.Background()
		_, _ = admin.Exec(bg, `DELETE FROM quantity_samples WHERE uuid = ANY($1) AND start_ts >= $2`, quantityUUIDs, start)
		_, _ = admin.Exec(bg, `DELETE FROM category_samples WHERE uuid = $1`, sleepUUID)
	}()

	window := SampleFilters{Start: start.Add(-time.Minute), End: start.Add(time.Hour), Limit: 10}

	t.Run("quantity", func(t *testing.T) {
		f := window
		f.Type = quantityType
		page, err := store.Samples(ctx, defaultUserID, f)
		if err != nil {
			t.Fatalf("Samples: %v", err)
		}
		if page.Kind != "quantity" || page.Unit == nil || *page.Unit != unit {
			t.Errorf("page = %+v", page)
		}
		if len(page.Samples) != 3 || page.NextOffset != 3 {
			t.Fatalf("samples = %d, nextOffset = %d", len(page.Samples), page.NextOffset)
		}
		first := page.Samples[0]
		if first.Value == nil || *first.Value != 60 || first.Start != start.UnixMilli() {
			t.Errorf("first sample = %+v", first)
		}
		if first.Source == nil || !strings.HasPrefix(*first.Source, "CodexSampleSource") {
			t.Errorf("source = %v", first.Source)
		}
		if first.Label != nil {
			t.Errorf("a quantity sample carries a label: %v", *first.Label)
		}
		// Ordered by start time, so the values run 60, 61, 62.
		for i, sample := range page.Samples {
			if sample.Value == nil || *sample.Value != 60+float64(i) {
				t.Errorf("sample %d = %+v, want ordering by start", i, sample)
			}
		}
	})

	t.Run("paging", func(t *testing.T) {
		f := window
		f.Type, f.Limit = quantityType, 2
		first, err := store.Samples(ctx, defaultUserID, f)
		if err != nil {
			t.Fatalf("Samples page 1: %v", err)
		}
		f.Offset = first.NextOffset
		second, err := store.Samples(ctx, defaultUserID, f)
		if err != nil {
			t.Fatalf("Samples page 2: %v", err)
		}
		if len(first.Samples) != 2 || first.NextOffset != 2 {
			t.Fatalf("page 1 = %d samples, nextOffset %d", len(first.Samples), first.NextOffset)
		}
		if len(second.Samples) != 1 || second.NextOffset != 3 {
			t.Fatalf("page 2 = %d samples, nextOffset %d", len(second.Samples), second.NextOffset)
		}
		if second.Samples[0].UUID == first.Samples[0].UUID {
			t.Error("the second page repeats the first")
		}
	})

	// The export path: a zero Limit means every row in the range, and an
	// error from the callback stops the scan at once.
	t.Run("streams the whole range without a limit", func(t *testing.T) {
		f := window
		f.Type, f.Limit = quantityType, 0
		meta, err := store.SampleType(ctx, quantityType)
		if err != nil {
			t.Fatalf("SampleType: %v", err)
		}
		if meta.Kind != "quantity" || meta.Unit == nil || *meta.Unit != unit {
			t.Fatalf("meta = %+v", meta)
		}

		var streamed []Sample
		if err := store.StreamSamples(ctx, defaultUserID, meta, f, func(s Sample) error {
			streamed = append(streamed, s)
			return nil
		}); err != nil {
			t.Fatalf("StreamSamples: %v", err)
		}
		if len(streamed) != 3 {
			t.Fatalf("streamed %d samples, want all 3", len(streamed))
		}

		stop := errors.New("stop")
		seen := 0
		if err := store.StreamSamples(ctx, defaultUserID, meta, f, func(Sample) error {
			seen++
			return stop
		}); !errors.Is(err, stop) {
			t.Fatalf("StreamSamples error = %v, want the callback's own error", err)
		}
		if seen != 1 {
			t.Fatalf("callback ran %d times after returning an error, want 1", seen)
		}
	})

	t.Run("category decodes its label", func(t *testing.T) {
		f := window
		f.Type = sleepTypeIdentifier
		page, err := store.Samples(ctx, defaultUserID, f)
		if err != nil {
			t.Fatalf("Samples: %v", err)
		}
		if page.Kind != "category" {
			t.Fatalf("kind = %q", page.Kind)
		}
		var found *Sample
		for i := range page.Samples {
			if page.Samples[i].UUID == sleepUUID {
				found = &page.Samples[i]
			}
		}
		if found == nil {
			t.Fatalf("the fixture sample is missing from %d samples", len(page.Samples))
		}
		if found.Label == nil || *found.Label != "Asleep Deep" {
			t.Errorf("label = %v, want the category_labels text", found.Label)
		}
		if found.Value == nil {
			t.Error("a category sample must carry its integer value too")
		}
	})

	t.Run("rejects types it cannot serve", func(t *testing.T) {
		f := window
		f.Type = "HKQuantityTypeIdentifierDefinitelyMissingForIntegrationTest"
		_, err := store.Samples(ctx, defaultUserID, f)
		wantRequestError(t, err, "unknown type")

		f.Type = workoutType
		_, err = store.Samples(ctx, defaultUserID, f)
		wantRequestError(t, err, "workout")
	})
}

func TestIntegrationWorkoutSeriesFiltersAndDownsamples(t *testing.T) {
	store, admin, ctx, cleanup := writeIntegrationStore(t)
	defer cleanup()

	suffix := time.Now().UTC().UnixNano()
	bpm, watts := "count/min", "W"
	heartType := fmt.Sprintf("HKQuantityTypeIdentifierCodexSeriesHR%d", suffix)
	heartID, dropHeart := ensureSampleType(t, ctx, admin, heartType, "quantity", &bpm)
	defer dropHeart()
	powerType := fmt.Sprintf("HKQuantityTypeIdentifierCodexSeriesPower%d", suffix)
	powerID, dropPower := ensureSampleType(t, ctx, admin, powerType, "quantity", &watts)
	defer dropPower()

	workoutUUID := fixtureUUID("abababab", suffix, 1)
	start := fixtureDay(2075, time.UTC, suffix).Add(9 * time.Hour)
	end := start.Add(30 * time.Minute)
	defer func() {
		bg := context.Background()
		_, _ = admin.Exec(bg, `DELETE FROM workout_series_points WHERE workout_uuid = $1`, workoutUUID)
		_, _ = admin.Exec(bg, `DELETE FROM workouts WHERE uuid = $1`, workoutUUID)
	}()
	if _, err := admin.Exec(ctx, `
		INSERT INTO workouts (uuid, activity_type, start_ts, end_ts, duration_s, user_id)
		VALUES ($1, 'HKWorkoutActivityTypeRunning', $2, $3, 1800, $4)`,
		workoutUUID, start, end, defaultUserID,
	); err != nil {
		t.Fatalf("insert workout: %v", err)
	}
	// 101 heart-rate points, one every ten seconds, rising 100..200.
	const points = 101
	for i := 0; i < points; i++ {
		if _, err := admin.Exec(ctx, `
			INSERT INTO workout_series_points (workout_uuid, type_id, ts, value, user_id)
			VALUES ($1, $2, $3, $4, $5)`,
			workoutUUID, heartID, start.Add(time.Duration(i)*10*time.Second), 100.0+float64(i), defaultUserID,
		); err != nil {
			t.Fatalf("insert heart-rate point: %v", err)
		}
	}
	if _, err := admin.Exec(ctx, `
		INSERT INTO workout_series_points (workout_uuid, type_id, ts, value, user_id)
		VALUES ($1, $2, $3, $4, $5)`,
		workoutUUID, powerID, start.Add(time.Minute), 240.0, defaultUserID,
	); err != nil {
		t.Fatalf("insert power point: %v", err)
	}

	all, err := store.WorkoutSeries(ctx, defaultUserID, workoutUUID, nil, 500)
	if err != nil {
		t.Fatalf("WorkoutSeries: %v", err)
	}
	if all == nil {
		t.Fatal("response is nil for an existing workout")
	}
	if len(all.Series) != 2 {
		t.Fatalf("series = %+v, want both streams", all.Series)
	}
	var heart *WorkoutSeries
	for i := range all.Series {
		if all.Series[i].Type == heartType {
			heart = &all.Series[i]
		}
	}
	if heart == nil {
		t.Fatalf("the heart-rate stream is missing: %+v", all.Series)
	}
	if heart.Unit == nil || *heart.Unit != bpm {
		t.Errorf("unit = %v, want %q from sample_types", heart.Unit, bpm)
	}
	if heart.TotalPoints != points || len(heart.Points) != points {
		t.Errorf("points = %d of %d, want all %d under the cap", len(heart.Points), heart.TotalPoints, points)
	}

	// Downsampled, the endpoints survive verbatim and the shape is kept.
	small, err := store.WorkoutSeries(ctx, defaultUserID, workoutUUID, []string{heartType}, 11)
	if err != nil {
		t.Fatalf("WorkoutSeries (downsampled): %v", err)
	}
	if len(small.Series) != 1 || small.Series[0].Type != heartType {
		t.Fatalf("types filter = %+v", small.Series)
	}
	got := small.Series[0]
	if got.TotalPoints != points || len(got.Points) != 11 {
		t.Fatalf("downsampled to %d points of %d, want 11", len(got.Points), got.TotalPoints)
	}
	if got.Points[0].V != 100 || got.Points[10].V != 200 {
		t.Errorf("endpoints = %v .. %v, want the true first and last reading", got.Points[0], got.Points[10])
	}
	if got.Points[0].T != start.UnixMilli() {
		t.Errorf("first point at %d, want the first reading's instant %d", got.Points[0].T, start.UnixMilli())
	}
	for i := 1; i < len(got.Points); i++ {
		if got.Points[i].T <= got.Points[i-1].T || got.Points[i].V <= got.Points[i-1].V {
			t.Fatalf("not monotonic at %d: %v -> %v", i, got.Points[i-1], got.Points[i])
		}
	}

	missing, err := store.WorkoutSeries(ctx, defaultUserID, fixtureUUID("bcbcbcbc", suffix, 2), nil, 500)
	if err != nil {
		t.Fatalf("WorkoutSeries (unknown): %v", err)
	}
	if missing != nil {
		t.Errorf("an unknown workout returned %+v, want nil", missing)
	}
}

func TestIntegrationStateOfMind(t *testing.T) {
	store, admin, ctx, cleanup := writeIntegrationStore(t)
	defer cleanup()

	loc, err := losAngelesLocation()
	if err != nil {
		t.Fatalf("losAngelesLocation: %v", err)
	}
	suffix := time.Now().UTC().UnixNano()
	dayStart := fixtureDay(2085, loc, suffix)
	day := dayStart.Format("2006-01-02")
	at := func(hour, minute int) time.Time {
		return time.Date(dayStart.Year(), dayStart.Month(), dayStart.Day(), hour, minute, 0, 0, loc)
	}
	// 23:30 local is already the next day in UTC: the row must be dated by
	// the server's zone, not by UTC.
	morning, late := at(8, 0), at(23, 30)

	first := fixtureUUID("fafafafa", suffix, 1)
	second := fixtureUUID("fafafafa", suffix, 2)
	defer func() {
		_, _ = admin.Exec(context.Background(), `DELETE FROM state_of_mind WHERE uuid = ANY($1)`, []string{first, second})
	}()
	if _, err := admin.Exec(ctx, `
		INSERT INTO state_of_mind (uuid, start_ts, end_ts, kind, valence, valence_class, labels, associations, user_id)
		VALUES ($1, $2, $2, 'momentaryEmotion', 0.5, 'slightlyPleasant', ARRAY['calm','grateful'], ARRAY['family'], $4),
		       ($3, $5, $5, 'dailyMood', -0.25, 'slightlyUnpleasant', NULL, NULL, $4)`,
		first, morning, second, defaultUserID, late,
	); err != nil {
		t.Fatalf("insert state_of_mind: %v", err)
	}

	entries, err := store.StateOfMind(ctx, defaultUserID, dayStart, dayStart.AddDate(0, 0, 1))
	if err != nil {
		t.Fatalf("StateOfMind: %v", err)
	}
	firstAt, secondAt := -1, -1
	for i, entry := range entries {
		switch entry.UUID {
		case first:
			firstAt = i
		case second:
			secondAt = i
		}
	}
	if firstAt < 0 || secondAt < 0 {
		t.Fatalf("entries = %#v, want both of this run's rows", entries)
	}
	if firstAt > secondAt {
		t.Errorf("the 23:30 entry came before the 08:00 one; rows must be oldest first")
	}
	e := entries[firstAt]
	if e.Date != day || e.Timestamp != morning.UnixMilli() || e.Kind != "momentaryEmotion" {
		t.Errorf("entry = %+v", e)
	}
	if e.Valence == nil || *e.Valence != 0.5 || e.ValenceClassification == nil || *e.ValenceClassification != "slightlyPleasant" {
		t.Errorf("valence = %v / %v", e.Valence, e.ValenceClassification)
	}
	if strings.Join(e.Labels, ",") != "calm,grateful" || strings.Join(e.Associations, ",") != "family" {
		t.Errorf("labels/associations = %v / %v", e.Labels, e.Associations)
	}
	// The late entry stays on its local day even though it is the next day
	// in UTC.
	if entries[secondAt].Date != day {
		t.Errorf("late entry date = %q, want the local day %s", entries[secondAt].Date, day)
	}
	// NULL arrays come back empty, never nil, so JSON renders [].
	if entries[secondAt].Labels == nil || len(entries[secondAt].Labels) != 0 {
		t.Errorf("labels = %#v, want an empty slice", entries[secondAt].Labels)
	}

	if _, err := store.StateOfMind(ctx, defaultUserID, dayStart.AddDate(-2, 0, 0), dayStart); err == nil {
		t.Error("a range over 366 days was accepted")
	} else {
		wantRequestError(t, err, "at most 366 days")
	}
}
