// SPDX-License-Identifier: Apache-2.0
package backupcontroller

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	strategyv1alpha1 "github.com/cozystack/cozystack/api/backups/strategy/v1alpha1"
	backupsv1alpha1 "github.com/cozystack/cozystack/api/backups/v1alpha1"
	"github.com/cozystack/cozystack/internal/backupcontroller/foundationdbapp"
	"github.com/cozystack/cozystack/internal/backupcontroller/foundationdbtypes"
)

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

func TestValidateFoundationDBApplicationRef(t *testing.T) {
	apps := foundationdbapp.GroupName
	other := "other.example.com"
	cases := []struct {
		name    string
		ref     corev1.TypedLocalObjectReference
		wantErr bool
	}{
		{"happy path with apps group", corev1.TypedLocalObjectReference{Kind: "FoundationDB", Name: "x", APIGroup: &apps}, false},
		{"empty apiGroup is accepted", corev1.TypedLocalObjectReference{Kind: "FoundationDB", Name: "x"}, false},
		{"foreign apiGroup rejected", corev1.TypedLocalObjectReference{Kind: "FoundationDB", Name: "x", APIGroup: &other}, true},
		{"wrong kind rejected", corev1.TypedLocalObjectReference{Kind: "Postgres", Name: "x", APIGroup: &apps}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFoundationDBApplicationRef(tc.ref)
			if tc.wantErr && err == nil {
				t.Fatalf("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cluster-name mapping (foundationdb release.prefix)
// ---------------------------------------------------------------------------

func TestFoundationDBClusterNameForApp(t *testing.T) {
	// The cozystack foundationdb ApplicationDefinition prefixes the
	// release name with "foundationdb-", so an app named "fdb-src"
	// renders apps.foundationdb.org/FoundationDBCluster
	// metadata.name=foundationdb-fdb-src.
	if got := foundationdbClusterNameForApp("fdb-src"); got != "foundationdb-fdb-src" {
		t.Errorf("foundationdbClusterNameForApp: got %q want foundationdb-fdb-src", got)
	}
}

// ---------------------------------------------------------------------------
// Templating
// ---------------------------------------------------------------------------

func TestRenderFoundationDBTemplate_TemplatingApplicationName(t *testing.T) {
	tmpl := strategyv1alpha1.FoundationDBTemplate{
		BlobStoreConfiguration: strategyv1alpha1.FoundationDBBlobStoreTemplate{
			AccountName:   "key@s3.example:9000",
			Bucket:        "{{ .Parameters.bucket }}",
			BackupName:    "{{ .Application.metadata.name }}-fdb",
			URLParameters: []string{"region={{ .Parameters.region }}"},
		},
		CustomParameters: []string{"--blob_credentials=/var/{{ .Application.metadata.name }}/creds"},
	}
	app := newFoundationDBApp("fdb-src", "tenant")
	got, err := renderFoundationDBTemplate(tmpl, app, map[string]string{"bucket": "shared-bucket", "region": "us-east-1"})
	if err != nil {
		t.Fatalf("renderFoundationDBTemplate: %v", err)
	}
	if got.BlobStoreConfiguration.Bucket != "shared-bucket" {
		t.Errorf("Bucket parameter not templated: got %q", got.BlobStoreConfiguration.Bucket)
	}
	if got.BlobStoreConfiguration.BackupName != "fdb-src-fdb" {
		t.Errorf("BackupName not templated: got %q", got.BlobStoreConfiguration.BackupName)
	}
	if len(got.BlobStoreConfiguration.URLParameters) != 1 || got.BlobStoreConfiguration.URLParameters[0] != "region=us-east-1" {
		t.Errorf("URLParameters not templated: got %#v", got.BlobStoreConfiguration.URLParameters)
	}
	if len(got.CustomParameters) != 1 || got.CustomParameters[0] != "--blob_credentials=/var/fdb-src/creds" {
		t.Errorf("CustomParameters not templated: got %#v", got.CustomParameters)
	}
}

// ---------------------------------------------------------------------------
// Backup-side ensure idempotency
// ---------------------------------------------------------------------------

func TestEnsureFoundationDBBackup_IdempotentByLabel(t *testing.T) {
	c := newFoundationDBStrategyTestClient(t)
	r := &BackupJobReconciler{Client: c, Scheme: c.Scheme()}
	job := newFoundationDBBackupJob("bj-1", "tenant")
	if err := c.Create(context.Background(), job); err != nil {
		t.Fatalf("seed BackupJob: %v", err)
	}
	rendered := newRenderedFoundationDBTemplate()
	clusterName := foundationdbClusterNameForApp("fdb-src")

	first, err := r.ensureFoundationDBBackup(context.Background(), job, clusterName, rendered)
	if err != nil {
		t.Fatalf("first ensureFoundationDBBackup: %v", err)
	}
	second, err := r.ensureFoundationDBBackup(context.Background(), job, clusterName, rendered)
	if err != nil {
		t.Fatalf("second ensureFoundationDBBackup: %v", err)
	}
	if first.Name != second.Name {
		t.Errorf("expected idempotent reuse: first=%q second=%q", first.Name, second.Name)
	}

	list := &foundationdbtypes.FoundationDBBackupList{}
	if err := c.List(context.Background(), list, client.InNamespace("tenant")); err != nil {
		t.Fatalf("list backups: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly one operator FoundationDBBackup CR, got %d", len(list.Items))
	}
	got := &list.Items[0]
	if got.Spec.ClusterName != "foundationdb-fdb-src" {
		t.Errorf("ClusterName: got %q want foundationdb-fdb-src", got.Spec.ClusterName)
	}
	if got.Spec.BackupState != foundationdbtypes.BackupStateRunning {
		t.Errorf("BackupState: got %q want %q", got.Spec.BackupState, foundationdbtypes.BackupStateRunning)
	}
	if got.Spec.BlobStoreConfiguration.BackupName != "bj-1" {
		// Template left BackupName empty, ensure path must default it to
		// the BackupJob name so each Cozystack BackupJob owns a discrete
		// blob-store directory.
		t.Errorf("BlobStoreConfiguration.BackupName: got %q want bj-1 (BackupJob name fallback)", got.Spec.BlobStoreConfiguration.BackupName)
	}
	if got.Labels[backupsv1alpha1.OwningJobNameLabel] != "bj-1" {
		t.Errorf("OwningJobName label missing or wrong: %v", got.Labels)
	}
	if got.Labels[foundationdbClusterLabel] != "foundationdb-fdb-src" {
		t.Errorf("foundationdbClusterLabel missing or wrong: %v", got.Labels)
	}
}

// TestEnsureFoundationDBBackup_StopsPriorRunning pins the FDB-specific
// invariant: the operator only permits one running backup directory per
// cluster, so an earlier BackupJob's CR has to flip backupState=Stopped
// before the new one can start. Without this gate the operator would
// reject the new FoundationDBBackup or both agents would race on the
// same blob-store path.
func TestEnsureFoundationDBBackup_StopsPriorRunning(t *testing.T) {
	clusterName := foundationdbClusterNameForApp("fdb-src")
	prior := &foundationdbtypes.FoundationDBBackup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenant", Name: "bj-0-aaa",
			Labels: map[string]string{
				backupsv1alpha1.OwningJobNameLabel:      "bj-0",
				backupsv1alpha1.OwningJobNamespaceLabel: "tenant",
				foundationdbClusterLabel:                clusterName,
			},
		},
		Spec: foundationdbtypes.FoundationDBBackupSpec{
			ClusterName: clusterName,
			BackupState: foundationdbtypes.BackupStateRunning,
		},
	}
	c := newFoundationDBStrategyTestClient(t, prior)
	r := &BackupJobReconciler{Client: c, Scheme: c.Scheme()}
	job := newFoundationDBBackupJob("bj-1", "tenant")
	rendered := newRenderedFoundationDBTemplate()

	if _, err := r.ensureFoundationDBBackup(context.Background(), job, clusterName, rendered); err != nil {
		t.Fatalf("ensureFoundationDBBackup: %v", err)
	}

	got := &foundationdbtypes.FoundationDBBackup{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "tenant", Name: prior.Name}, got); err != nil {
		t.Fatalf("get prior CR: %v", err)
	}
	if got.Spec.BackupState != foundationdbtypes.BackupStateStopped {
		t.Errorf("prior FoundationDBBackup must be flipped to Stopped; got %q", got.Spec.BackupState)
	}
}

// ---------------------------------------------------------------------------
// Restore-side ensure idempotency + target rewrite
// ---------------------------------------------------------------------------

func TestEnsureFoundationDBRestore_IdempotentByLabel(t *testing.T) {
	c := newFoundationDBStrategyTestClient(t)
	r := &RestoreJobReconciler{Client: c, Scheme: c.Scheme()}
	rj := newFoundationDBRestoreJob("rj-1", "tenant")
	if err := c.Create(context.Background(), rj); err != nil {
		t.Fatalf("seed RestoreJob: %v", err)
	}
	blob := foundationdbtypes.BlobStoreConfiguration{
		AccountName: "key@s3.example:9000",
		Bucket:      "bkt",
		BackupName:  "bj-1",
	}

	first, err := r.ensureFoundationDBRestore(context.Background(), rj, "foundationdb-fdb-target", blob, nil, nil)
	if err != nil {
		t.Fatalf("first ensureFoundationDBRestore: %v", err)
	}
	second, err := r.ensureFoundationDBRestore(context.Background(), rj, "foundationdb-fdb-target", blob, nil, nil)
	if err != nil {
		t.Fatalf("second ensureFoundationDBRestore: %v", err)
	}
	if first.Name != second.Name {
		t.Errorf("expected idempotent reuse: first=%q second=%q", first.Name, second.Name)
	}
	list := &foundationdbtypes.FoundationDBRestoreList{}
	if err := c.List(context.Background(), list, client.InNamespace("tenant")); err != nil {
		t.Fatalf("list restores: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected exactly one operator FoundationDBRestore CR, got %d", len(list.Items))
	}
	got := &list.Items[0]
	if got.Spec.DestinationClusterName != "foundationdb-fdb-target" {
		t.Errorf("DestinationClusterName: got %q want foundationdb-fdb-target", got.Spec.DestinationClusterName)
	}
	if !apiequality.Semantic.DeepEqual(got.Spec.BlobStoreConfiguration, blob) {
		t.Errorf("BlobStoreConfiguration: got %#v want %#v", got.Spec.BlobStoreConfiguration, blob)
	}
}

// ---------------------------------------------------------------------------
// resolveFoundationDBRestoreTarget (in-place vs to-copy)
// ---------------------------------------------------------------------------

func TestResolveFoundationDBRestoreTarget(t *testing.T) {
	apps := foundationdbapp.GroupName
	backup := &backupsv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "src-bk"},
		Spec: backupsv1alpha1.BackupSpec{
			ApplicationRef: corev1.TypedLocalObjectReference{
				Kind: "FoundationDB", Name: "fdb-src", APIGroup: &apps,
			},
		},
	}

	t.Run("in-place: missing targetApplicationRef inherits source", func(t *testing.T) {
		rj := &backupsv1alpha1.RestoreJob{Spec: backupsv1alpha1.RestoreJobSpec{}}
		r := &RestoreJobReconciler{}
		got := r.resolveFoundationDBRestoreTarget(rj, backup)
		if got.AppName != "fdb-src" {
			t.Errorf("in-place AppName: got %q want fdb-src", got.AppName)
		}
		if got.Kind != "FoundationDB" {
			t.Errorf("in-place Kind: got %q want FoundationDB", got.Kind)
		}
	})

	t.Run("to-copy: targetApplicationRef wins over source", func(t *testing.T) {
		rj := &backupsv1alpha1.RestoreJob{
			Spec: backupsv1alpha1.RestoreJobSpec{
				TargetApplicationRef: &corev1.TypedLocalObjectReference{
					Kind: "FoundationDB", Name: "fdb-target", APIGroup: &apps,
				},
			},
		}
		r := &RestoreJobReconciler{}
		got := r.resolveFoundationDBRestoreTarget(rj, backup)
		if got.AppName != "fdb-target" {
			t.Errorf("to-copy AppName: got %q want fdb-target", got.AppName)
		}
	})
}

// ---------------------------------------------------------------------------
// Snapshot persistence round-trip
// ---------------------------------------------------------------------------

func TestMarshalDecodeFoundationDBBackupSnapshot_RoundTrip(t *testing.T) {
	rendered := newRenderedFoundationDBTemplate()
	parameters := map[string]string{"region": "us-east-1"}
	fdbBackup := &foundationdbtypes.FoundationDBBackup{
		Spec: foundationdbtypes.FoundationDBBackupSpec{
			BlobStoreConfiguration: foundationdbtypes.BlobStoreConfiguration{
				BackupName: "bj-1",
			},
		},
	}
	raw, err := marshalFoundationDBBackupSnapshot(newFoundationDBApp("fdb-src", "tenant"), rendered, parameters, fdbBackup)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if raw == nil || len(raw.Raw) == 0 {
		t.Fatalf("expected non-empty RawExtension")
	}
	var snap foundationdbBackupSnapshot
	if err := json.Unmarshal(raw.Raw, &snap); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snap.Kind != foundationdbBackupSnapshotKind {
		t.Errorf("snapshot kind: got %q want %q", snap.Kind, foundationdbBackupSnapshotKind)
	}
	// Template left BackupName empty; marshal must backfill it from the
	// per-run FoundationDBBackup so restore-time reuse picks the right
	// blob-store path.
	if snap.Storage.BackupName != "bj-1" {
		t.Errorf("snapshot Storage.BackupName: got %q want bj-1 (backfilled from FoundationDBBackup)", snap.Storage.BackupName)
	}
	if got := snap.Parameters["region"]; got != "us-east-1" {
		t.Errorf("snapshot parameters: got %#v", snap.Parameters)
	}

	decoded, err := decodeFoundationDBBackupSnapshot(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded == nil || decoded.Kind != foundationdbBackupSnapshotKind {
		t.Errorf("decoded snapshot mismatch: %#v", decoded)
	}
}

// ---------------------------------------------------------------------------
// resolveFoundationDBRestoreBlob: snapshot vs driverMetadata fallback
// ---------------------------------------------------------------------------

func TestResolveFoundationDBRestoreBlob_SnapshotWinsOverMetadata(t *testing.T) {
	snap := &foundationdbBackupSnapshot{
		Storage: strategyv1alpha1.FoundationDBBlobStoreTemplate{
			AccountName: "from-snap",
			Bucket:      "snap-bucket",
			BackupName:  "snap-name",
		},
	}
	backup := &backupsv1alpha1.Backup{
		Spec: backupsv1alpha1.BackupSpec{
			DriverMetadata: map[string]string{
				foundationdbAccountNameKey:    "from-md",
				foundationdbBucketKey:         "md-bucket",
				foundationdbBlobBackupNameKey: "md-name",
			},
		},
	}
	got, ok := resolveFoundationDBRestoreBlob(backup, snap)
	if !ok {
		t.Fatalf("expected ok=true with snapshot")
	}
	if got.AccountName != "from-snap" || got.Bucket != "snap-bucket" || got.BackupName != "snap-name" {
		t.Errorf("snapshot must win over driverMetadata; got %#v", got)
	}
}

func TestResolveFoundationDBRestoreBlob_MetadataFallback(t *testing.T) {
	backup := &backupsv1alpha1.Backup{
		Spec: backupsv1alpha1.BackupSpec{
			DriverMetadata: map[string]string{
				foundationdbAccountNameKey:    "from-md",
				foundationdbBucketKey:         "md-bucket",
				foundationdbBlobBackupNameKey: "md-name",
			},
		},
	}
	got, ok := resolveFoundationDBRestoreBlob(backup, nil)
	if !ok {
		t.Fatalf("expected ok=true with driverMetadata fallback")
	}
	if got.AccountName != "from-md" || got.Bucket != "md-bucket" || got.BackupName != "md-name" {
		t.Errorf("driverMetadata fallback: got %#v", got)
	}
}

func TestResolveFoundationDBRestoreBlob_EmptyReturnsFalse(t *testing.T) {
	backup := &backupsv1alpha1.Backup{Spec: backupsv1alpha1.BackupSpec{}}
	_, ok := resolveFoundationDBRestoreBlob(backup, nil)
	if ok {
		t.Fatalf("empty backup must return ok=false")
	}
}

// ---------------------------------------------------------------------------
// Backup readiness
// ---------------------------------------------------------------------------

func TestFoundationDBBackupReady(t *testing.T) {
	t.Run("nil backup is not ready", func(t *testing.T) {
		if foundationdbBackupReady(nil) {
			t.Fatalf("nil backup must not be ready")
		}
	})
	t.Run("missing backupDetails is not ready", func(t *testing.T) {
		if foundationdbBackupReady(&foundationdbtypes.FoundationDBBackup{}) {
			t.Fatalf("backup without details must not be ready")
		}
	})
	t.Run("not running is not ready", func(t *testing.T) {
		b := &foundationdbtypes.FoundationDBBackup{Status: foundationdbtypes.FoundationDBBackupStatus{
			BackupDetails: &foundationdbtypes.BackupDetails{Running: false, SnapshotTime: 100},
		}}
		if foundationdbBackupReady(b) {
			t.Fatalf("running=false must not be ready")
		}
	})
	t.Run("snapshotTime=0 is not ready", func(t *testing.T) {
		b := &foundationdbtypes.FoundationDBBackup{Status: foundationdbtypes.FoundationDBBackupStatus{
			BackupDetails: &foundationdbtypes.BackupDetails{Running: true, SnapshotTime: 0},
		}}
		if foundationdbBackupReady(b) {
			t.Fatalf("snapshotTime=0 must not be ready")
		}
	})
	t.Run("generations not reconciled is not ready", func(t *testing.T) {
		b := &foundationdbtypes.FoundationDBBackup{
			ObjectMeta: metav1.ObjectMeta{Generation: 2},
			Status: foundationdbtypes.FoundationDBBackupStatus{
				BackupDetails: &foundationdbtypes.BackupDetails{Running: true, SnapshotTime: 100},
				Generations:   &foundationdbtypes.BackupGenerationStatus{Reconciled: 1},
			},
		}
		if foundationdbBackupReady(b) {
			t.Fatalf("generations.reconciled < generation must not be ready")
		}
	})
	t.Run("ready when running + snapshotTime > 0 + reconciled", func(t *testing.T) {
		b := &foundationdbtypes.FoundationDBBackup{
			ObjectMeta: metav1.ObjectMeta{Generation: 1},
			Status: foundationdbtypes.FoundationDBBackupStatus{
				BackupDetails: &foundationdbtypes.BackupDetails{Running: true, SnapshotTime: 100},
				Generations:   &foundationdbtypes.BackupGenerationStatus{Reconciled: 1},
			},
		}
		if !foundationdbBackupReady(b) {
			t.Fatalf("expected ready=true")
		}
	})
}

// ---------------------------------------------------------------------------
// Restore options + deadline
// ---------------------------------------------------------------------------

func TestParseFoundationDBRestoreOptions(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    int64
		wantErr bool
	}{
		{"nil blob", "", 0, false},
		{"missing fields", `{"foo":"bar"}`, 0, false},
		{"override", `{"restoreTimeoutSeconds":7200}`, 7200, false},
		{"malformed", `not-json`, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ext *runtime.RawExtension
			if tc.raw != "" {
				ext = &runtime.RawExtension{Raw: []byte(tc.raw)}
			}
			got, err := parseFoundationDBRestoreOptions(ext)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected parse error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected parse error: %v", err)
			}
			if got.RestoreTimeoutSeconds != tc.want {
				t.Errorf("RestoreTimeoutSeconds: got %d want %d", got.RestoreTimeoutSeconds, tc.want)
			}
		})
	}
}

func TestFoundationDBRestoreOptions_EffectiveDeadline(t *testing.T) {
	cases := []struct {
		name string
		opts FoundationDBRestoreOptions
		want time.Duration
	}{
		{"unset", FoundationDBRestoreOptions{}, foundationdbDefaultRestoreDeadline},
		{"zero", FoundationDBRestoreOptions{RestoreTimeoutSeconds: 0}, foundationdbDefaultRestoreDeadline},
		{"negative", FoundationDBRestoreOptions{RestoreTimeoutSeconds: -1}, foundationdbDefaultRestoreDeadline},
		{"override", FoundationDBRestoreOptions{RestoreTimeoutSeconds: 7200}, 2 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.opts.effectiveRestoreDeadline(); got != tc.want {
				t.Errorf("got %s want %s", got, tc.want)
			}
		})
	}
}

func TestFoundationDBBackupDeadlineExceeded(t *testing.T) {
	if foundationdbBackupDeadlineExceeded(nil) {
		t.Fatalf("nil StartedAt must not trip the gate (deadline starts when StartedAt is set)")
	}
	recent := metav1.NewTime(time.Now())
	if foundationdbBackupDeadlineExceeded(&recent) {
		t.Fatalf("recent StartedAt must not trip the gate")
	}
	old := metav1.NewTime(time.Now().Add(-2 * foundationdbDefaultBackupDeadline))
	if !foundationdbBackupDeadlineExceeded(&old) {
		t.Fatalf("StartedAt past default deadline must trip the gate")
	}
}

// ---------------------------------------------------------------------------
// URI synthesis
// ---------------------------------------------------------------------------

func TestFoundationDBBackupURI_PrefersOperatorReportedURL(t *testing.T) {
	fdb := &foundationdbtypes.FoundationDBBackup{
		Spec: foundationdbtypes.FoundationDBBackupSpec{
			BlobStoreConfiguration: foundationdbtypes.BlobStoreConfiguration{
				AccountName: "key@s3.example:9000",
				Bucket:      "bkt",
				BackupName:  "bj-1",
			},
		},
		Status: foundationdbtypes.FoundationDBBackupStatus{
			BackupDetails: &foundationdbtypes.BackupDetails{URL: "blobstore://bkt/bj-1?secure_connection=0"},
		},
	}
	if got, want := foundationdbBackupURI(fdb), "blobstore://bkt/bj-1?secure_connection=0"; got != want {
		t.Errorf("URI: got %q want %q", got, want)
	}
}

func TestFoundationDBBackupURI_FallsBackToSpec(t *testing.T) {
	fdb := &foundationdbtypes.FoundationDBBackup{
		Spec: foundationdbtypes.FoundationDBBackupSpec{
			BlobStoreConfiguration: foundationdbtypes.BlobStoreConfiguration{
				AccountName: "key@s3.example:9000",
				Bucket:      "bkt",
				BackupName:  "bj-1",
			},
		},
	}
	if got, want := foundationdbBackupURI(fdb), "blobstore://bkt/bj-1"; got != want {
		t.Errorf("URI: got %q want %q", got, want)
	}
}

func TestFoundationDBBackupURI_EmptyIsEmpty(t *testing.T) {
	if got := foundationdbBackupURI(nil); got != "" {
		t.Errorf("nil URI: got %q want \"\"", got)
	}
	if got := foundationdbBackupURI(&foundationdbtypes.FoundationDBBackup{}); got != "" {
		t.Errorf("empty URI: got %q want \"\"", got)
	}
}

// ---------------------------------------------------------------------------
// Restore reconcile: target cluster transient + kind validation
// ---------------------------------------------------------------------------

func TestReconcileFoundationDBRestore_TargetClusterNotFoundIsTransient(t *testing.T) {
	apps := foundationdbapp.GroupName
	now := metav1.Now()
	cozyBackup := &backupsv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "src-cozy-bk"},
		Spec: backupsv1alpha1.BackupSpec{
			ApplicationRef: corev1.TypedLocalObjectReference{
				Kind: "FoundationDB", Name: "fdb-src", APIGroup: &apps,
			},
			DriverMetadata: map[string]string{
				foundationdbAccountNameKey:    "key@s3.example:9000",
				foundationdbBucketKey:         "bkt",
				foundationdbBlobBackupNameKey: "src-op-bk",
			},
		},
	}
	rj := &backupsv1alpha1.RestoreJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "rj-target-missing"},
		Spec: backupsv1alpha1.RestoreJobSpec{
			BackupRef: corev1.LocalObjectReference{Name: cozyBackup.Name},
			TargetApplicationRef: &corev1.TypedLocalObjectReference{
				Kind: "FoundationDB", Name: "target", APIGroup: &apps,
			},
		},
		Status: backupsv1alpha1.RestoreJobStatus{
			StartedAt: &now,
		},
	}
	c := newFoundationDBStrategyTestClient(t, cozyBackup, rj)
	r := &RestoreJobReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}

	res, err := r.reconcileFoundationDBRestore(context.Background(), rj, cozyBackup)
	if err != nil {
		t.Fatalf("reconcileFoundationDBRestore: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("missing target cluster must produce transient requeue, got %+v", res)
	}
	if rj.Status.Phase == backupsv1alpha1.RestoreJobPhaseFailed {
		t.Errorf("missing target cluster must NOT mark RestoreJob Failed; got phase=%q", rj.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(rj.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("expected Ready condition, got none")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != "TargetFoundationDBClusterNotReady" {
		t.Errorf("expected Ready=False/TargetFoundationDBClusterNotReady, got %s/%s", cond.Status, cond.Reason)
	}
}

func TestReconcileFoundationDBRestore_KindCheckedBeforeBlobLookup(t *testing.T) {
	apps := foundationdbapp.GroupName
	now := metav1.Now()
	cozyBackup := &backupsv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "src-cozy-bk"},
		Spec: backupsv1alpha1.BackupSpec{
			ApplicationRef: corev1.TypedLocalObjectReference{
				Kind: "FoundationDB", Name: "fdb-src", APIGroup: &apps,
			},
			DriverMetadata: map[string]string{
				foundationdbAccountNameKey:    "key@s3.example:9000",
				foundationdbBucketKey:         "bkt",
				foundationdbBlobBackupNameKey: "src-op-bk",
			},
		},
	}
	rj := &backupsv1alpha1.RestoreJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant", Name: "rj-bad-kind"},
		Spec: backupsv1alpha1.RestoreJobSpec{
			BackupRef: corev1.LocalObjectReference{Name: cozyBackup.Name},
			TargetApplicationRef: &corev1.TypedLocalObjectReference{
				Kind: "Postgres", Name: "wrong", APIGroup: &apps,
			},
		},
		Status: backupsv1alpha1.RestoreJobStatus{StartedAt: &now},
	}
	c := newFoundationDBStrategyTestClient(t, cozyBackup, rj)
	r := &RestoreJobReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}

	if _, err := r.reconcileFoundationDBRestore(context.Background(), rj, cozyBackup); err != nil {
		t.Fatalf("reconcileFoundationDBRestore: %v", err)
	}
	if rj.Status.Phase != backupsv1alpha1.RestoreJobPhaseFailed {
		t.Fatalf("expected RestoreJob to be Failed on bad TargetApplicationRef.Kind, got phase=%q", rj.Status.Phase)
	}
	cond := apimeta.FindStatusCondition(rj.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("expected Ready condition, got none")
	}
	if !strings.Contains(cond.Message, "applicationRef.kind") {
		t.Errorf("expected Kind-rejection message, got %q", cond.Message)
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func newFoundationDBStrategyTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = backupsv1alpha1.AddToScheme(s)
	_ = strategyv1alpha1.AddToScheme(s)
	_ = foundationdbtypes.AddToScheme(s)
	_ = foundationdbapp.AddToScheme(s)
	return clientfake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&backupsv1alpha1.BackupJob{}, &backupsv1alpha1.RestoreJob{}, &backupsv1alpha1.Backup{}).
		Build()
}

func newFoundationDBApp(name, namespace string) *foundationdbapp.FoundationDB {
	return &foundationdbapp.FoundationDB{
		TypeMeta: metav1.TypeMeta{
			APIVersion: foundationdbapp.GroupVersion.String(),
			Kind:       foundationdbapp.Kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func newFoundationDBBackupJob(name, namespace string) *backupsv1alpha1.BackupJob {
	apps := foundationdbapp.GroupName
	return &backupsv1alpha1.BackupJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: backupsv1alpha1.BackupJobSpec{
			ApplicationRef: corev1.TypedLocalObjectReference{
				Kind: "FoundationDB", Name: "fdb-src", APIGroup: &apps,
			},
		},
	}
}

func newFoundationDBRestoreJob(name, namespace string) *backupsv1alpha1.RestoreJob {
	return &backupsv1alpha1.RestoreJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: backupsv1alpha1.RestoreJobSpec{
			BackupRef: corev1.LocalObjectReference{Name: "src-backup"},
		},
	}
}

func newRenderedFoundationDBTemplate() *strategyv1alpha1.FoundationDBTemplate {
	return &strategyv1alpha1.FoundationDBTemplate{
		BlobStoreConfiguration: strategyv1alpha1.FoundationDBBlobStoreTemplate{
			AccountName: "key@s3.example:9000",
			Bucket:      "bkt",
		},
		CustomParameters: []string{"--blob_credentials=/var/fdb-blob-credentials/credentials"},
	}
}
