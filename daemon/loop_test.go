package daemon

import (
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"

	"context"

	"github.com/weaveworks/flux"
	"github.com/weaveworks/flux/cluster"
	"github.com/weaveworks/flux/cluster/kubernetes"
	"github.com/weaveworks/flux/cluster/kubernetes/testfiles"
	"github.com/weaveworks/flux/event"
	"github.com/weaveworks/flux/git"
	"github.com/weaveworks/flux/git/gittest"
	"github.com/weaveworks/flux/gpg/gpgtest"
	"github.com/weaveworks/flux/job"
	registryMock "github.com/weaveworks/flux/registry/mock"
)

const (
	gitPath     = ""
	gitSyncTag  = "flux-sync"
	gitNotesRef = "flux"
	gitUser     = "Weave Flux"
	gitEmail    = "support@weave.works"
)

var (
	k8s                 *cluster.Mock
	events              *mockEventWriter
	gitConfig           = git.Config{
		Branch:    "master",
		SyncTag:   gitSyncTag,
		NotesRef:  gitNotesRef,
		UserName:  gitUser,
		UserEmail: gitEmail,
	}
	loopVars           = LoopVars{
		GitOpTimeout:        5 * time.Second,
	}
)

func daemon(t *testing.T, config git.Config, vars LoopVars) (*Daemon, func()) {
	repo, repoCleanup := gittest.Repo(t, config.SigningKey)

	k8s = &cluster.Mock{}
	k8s.ExportFunc = func() ([]byte, error) { return nil, nil }

	events = &mockEventWriter{}

	wg := &sync.WaitGroup{}
	shutdown := make(chan struct{})

	if err := repo.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}

	jobs := job.NewQueue(shutdown, wg)
	d := &Daemon{
		Cluster:        k8s,
		Manifests:      &kubernetes.Manifests{Namespacer: alwaysDefault},
		Registry:       &registryMock.Registry{},
		Repo:           repo,
		GitConfig:      config,
		Jobs:           jobs,
		JobStatusCache: &job.StatusCache{Size: 100},
		EventWriter:    events,
		Logger:         log.NewLogfmtLogger(os.Stdout),
		LoopVars: 		&vars,
	}
	return d, func() {
		close(shutdown)
		wg.Wait()
		repoCleanup()
		k8s = nil
		events = nil
	}
}

func TestPullAndSync_InitialSync(t *testing.T) {
	// No tag
	// No notes
	d, cleanup := daemon(t, gitConfig, loopVars)
	defer cleanup()

	syncCalled := 0
	var syncDef *cluster.SyncSet
	expectedResourceIDs := flux.ResourceIDs{}
	for id, _ := range testfiles.ResourceMap {
		expectedResourceIDs = append(expectedResourceIDs, id)
	}
	expectedResourceIDs.Sort()
	k8s.SyncFunc = func(def cluster.SyncSet) error {
		syncCalled++
		syncDef = &def
		return nil
	}
	var (
		logger                   = log.NewLogfmtLogger(ioutil.Discard)
		lastKnownSyncTagRev      string
		warnedAboutSyncTagChange bool
	)
	d.doSync(logger, &lastKnownSyncTagRev, &warnedAboutSyncTagChange)

	// It applies everything
	if syncCalled != 1 {
		t.Errorf("Sync was not called once, was called %d times", syncCalled)
	} else if syncDef == nil {
		t.Errorf("Sync was called with a nil syncDef")
	}

	// The emitted event has all service ids
	es, err := events.AllEvents(time.Time{}, -1, time.Time{})
	if err != nil {
		t.Error(err)
	} else if len(es) != 1 {
		t.Errorf("Unexpected events: %#v", es)
	} else if es[0].Type != event.EventSync {
		t.Errorf("Unexpected event type: %#v", es[0])
	} else {
		gotResourceIDs := es[0].ServiceIDs
		flux.ResourceIDs(gotResourceIDs).Sort()
		if !reflect.DeepEqual(gotResourceIDs, []flux.ResourceID(expectedResourceIDs)) {
			t.Errorf("Unexpected event service ids: %#v, expected: %#v", gotResourceIDs, expectedResourceIDs)
		}
	}
	// It creates the tag at HEAD
	if err := d.Repo.Refresh(context.Background()); err != nil {
		t.Errorf("pulling sync tag: %v", err)
	} else if revs, err := d.Repo.CommitsBefore(context.Background(), gitSyncTag); err != nil {
		t.Errorf("finding revisions before sync tag: %v", err)
	} else if len(revs) <= 0 {
		t.Errorf("Found no revisions before the sync tag")
	}
}

func TestPullAndSync_InitialSync_InvalidSignature(t *testing.T) {
	vars := loopVars
	vars.GitVerifySignatures = true

	d, cleanup := daemon(t, gitConfig, vars)
	defer cleanup()

	syncCalled := 0
	k8s.SyncFunc = func(def cluster.SyncSet) error {
		syncCalled++
		return nil
	}
	var (
		logger                   = log.NewLogfmtLogger(ioutil.Discard)
		lastKnownSyncTagRev      string
		warnedAboutSyncTagChange bool
	)
	err := d.doSync(logger, &lastKnownSyncTagRev, &warnedAboutSyncTagChange)
	if err == nil {
		t.Error("Expected error but got nil")
	}

	// It does not apply anything
	if syncCalled != 0 {
		t.Errorf("Sync was called %d times, should not be called at all", syncCalled)
	}
}

func TestPullAndSync_InitialSync_ValidAndInvalidSignature(t *testing.T) {
	gpgHome, gpgKey, gpgCleanup := gpgtest.GPGKey(t)
	defer gpgCleanup()

	os.Setenv("GNUPGHOME", gpgHome)
	defer os.Unsetenv("GNUPGHOME")

	config := gitConfig
	config.SigningKey = gpgKey

	vars := loopVars
	vars.GitVerifySignatures = true

	d, cleanup := daemon(t, config, vars)
	defer cleanup()

	ctx := context.Background()
	var expectedRevision string
	// Create a commit with a temp GPG key unknown to the daemon
	err := d.WithClone(ctx, func(checkout *git.Checkout) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var err error
		expectedRevision, err = checkout.HeadRevision(ctx)
		if err != nil {
			return err
		}

		// Push some new changes
		dirs := checkout.ManifestDirs()
		err = cluster.UpdateManifest(d.Manifests, checkout.Dir(), dirs, flux.MustParseResourceID("default:deployment/helloworld"), func(def []byte) ([]byte, error) {
			// Empty the file
			return []byte(""), nil
		})
		if err != nil {
			return err
		}

		tmpGpgHome, tmpGpgKey, tmpGpgCleanup := gpgtest.GPGKey(t)
		defer tmpGpgCleanup()
		os.Setenv("GNUPGHOME", tmpGpgHome)
		defer os.Setenv("GNUPGHOME", gpgHome)

		commitAction := git.CommitAction{Author: "", Message: "test commit", SigningKey: tmpGpgKey}
		err = checkout.CommitAndPush(ctx, commitAction, nil)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = d.Repo.Refresh(ctx)
	if err != nil {
		t.Error(err)
	}

	syncCalled := 0
	var syncDef *cluster.SyncSet
	expectedResourceIDs := flux.ResourceIDs{}
	for id, _ := range testfiles.ResourceMap {
		expectedResourceIDs = append(expectedResourceIDs, id)
	}
	expectedResourceIDs.Sort()
	k8s.SyncFunc = func(def cluster.SyncSet) error {
		syncCalled++
		syncDef = &def
		return nil
	}
	var (
		logger                   = log.NewLogfmtLogger(ioutil.Discard)
		lastKnownSyncTagRev      string
		warnedAboutSyncTagChange bool
	)
	err = d.doSync(logger, &lastKnownSyncTagRev, &warnedAboutSyncTagChange)
	if err != nil {
		t.Fatal(err)
	}

	// It applies
	if syncCalled != 1 {
		t.Errorf("Sync was not called once, was called %d times", syncCalled)
	} else if syncDef == nil {
		t.Errorf("Sync was called with a nil syncDef")
	}

	// The emitted event has all service ids (we removed one in a commit with unknown signature)
	es, err := events.AllEvents(time.Time{}, -1, time.Time{})
	if err != nil {
		t.Error(err)
	} else if len(es) != 1 {
		t.Errorf("Unexpected events: %#v", es)
	} else if es[0].Type != event.EventSync {
		t.Errorf("Unexpected event type: %#v", es[0])
	} else {
		gotResourceIDs := es[0].ServiceIDs
		flux.ResourceIDs(gotResourceIDs).Sort()
		if !reflect.DeepEqual(gotResourceIDs, []flux.ResourceID(expectedResourceIDs)) {
			t.Errorf("Unexpected event service ids: %#v, expected: %#v", gotResourceIDs, expectedResourceIDs)
		}
	}

	// It creates the correct tag
	if err := d.Repo.Refresh(context.Background()); err != nil {
		t.Errorf("pulling sync tag: %v", err)
	} else if actualRevision, err := d.Repo.Revision(ctx, gitSyncTag); err != nil {
		t.Errorf("finding revisions for sync tag: %v", err)
	} else if actualRevision != expectedRevision {
		t.Errorf("expected revision %s got %s", expectedRevision, actualRevision)
	}
}

func TestDoSync_NoNewCommits(t *testing.T) {
	d, cleanup := daemon(t, gitConfig, loopVars)
	defer cleanup()

	ctx := context.Background()
	err := d.WithClone(ctx, func(co *git.Checkout) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		tagAction := git.TagAction{
			Revision: "HEAD",
			Message:  "Sync pointer",
		}
		return co.MoveSyncTagAndPush(ctx, tagAction)
	})
	if err != nil {
		t.Fatal(err)
	}

	// NB this would usually trigger a sync in a running loop; but we
	// have not run the loop.
	if err = d.Repo.Refresh(ctx); err != nil {
		t.Error(err)
	}

	syncCalled := 0
	var syncDef *cluster.SyncSet
	expectedResourceIDs := flux.ResourceIDs{}
	for id, _ := range testfiles.ResourceMap {
		expectedResourceIDs = append(expectedResourceIDs, id)
	}
	expectedResourceIDs.Sort()
	k8s.SyncFunc = func(def cluster.SyncSet) error {
		syncCalled++
		syncDef = &def
		return nil
	}
	var (
		logger                   = log.NewLogfmtLogger(ioutil.Discard)
		lastKnownSyncTagRev      string
		warnedAboutSyncTagChange bool
	)
	if err := d.doSync(logger, &lastKnownSyncTagRev, &warnedAboutSyncTagChange); err != nil {
		t.Error(err)
	}

	// It applies everything
	if syncCalled != 1 {
		t.Errorf("Sync was not called once, was called %d times", syncCalled)
	} else if syncDef == nil {
		t.Errorf("Sync was called with a nil syncDef")
	}

	// The emitted event has no service ids
	es, err := events.AllEvents(time.Time{}, -1, time.Time{})
	if err != nil {
		t.Error(err)
	} else if len(es) != 0 {
		t.Errorf("Unexpected events: %#v", es)
	}

	// It doesn't move the tag
	oldRevs, err := d.Repo.CommitsBefore(ctx, gitSyncTag)
	if err != nil {
		t.Fatal(err)
	}

	if revs, err := d.Repo.CommitsBefore(ctx, gitSyncTag); err != nil {
		t.Errorf("finding revisions before sync tag: %v", err)
	} else if !reflect.DeepEqual(revs, oldRevs) {
		t.Errorf("Should have kept the sync tag at HEAD")
	}
}

func TestDoSync_WithNewCommit(t *testing.T) {
	d, cleanup := daemon(t, gitConfig, loopVars)
	defer cleanup()

	ctx := context.Background()
	// Set the sync tag to head
	var oldRevision, newRevision string
	err := d.WithClone(ctx, func(checkout *git.Checkout) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var err error
		tagAction := git.TagAction{
			Revision: "HEAD",
			Message:  "Sync pointer",
		}
		err = checkout.MoveSyncTagAndPush(ctx, tagAction)
		if err != nil {
			return err
		}
		oldRevision, err = checkout.HeadRevision(ctx)
		if err != nil {
			return err
		}
		// Push some new changes
		dirs := checkout.ManifestDirs()
		err = cluster.UpdateManifest(d.Manifests, checkout.Dir(), dirs, flux.MustParseResourceID("default:deployment/helloworld"), func(def []byte) ([]byte, error) {
			// A simple modification so we have changes to push
			return []byte(strings.Replace(string(def), "replicas: 5", "replicas: 4", -1)), nil
		})
		if err != nil {
			return err
		}

		commitAction := git.CommitAction{Author: "", Message: "test commit"}
		err = checkout.CommitAndPush(ctx, commitAction, nil)
		if err != nil {
			return err
		}
		newRevision, err = checkout.HeadRevision(ctx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	err = d.Repo.Refresh(ctx)
	if err != nil {
		t.Error(err)
	}

	syncCalled := 0
	var syncDef *cluster.SyncSet
	expectedResourceIDs := flux.ResourceIDs{}
	for id, _ := range testfiles.ResourceMap {
		expectedResourceIDs = append(expectedResourceIDs, id)
	}
	expectedResourceIDs.Sort()
	k8s.SyncFunc = func(def cluster.SyncSet) error {
		syncCalled++
		syncDef = &def
		return nil
	}
	var (
		logger                   = log.NewLogfmtLogger(ioutil.Discard)
		lastKnownSyncTagRev      string
		warnedAboutSyncTagChange bool
	)
	d.doSync(logger, &lastKnownSyncTagRev, &warnedAboutSyncTagChange)

	// It applies everything
	if syncCalled != 1 {
		t.Errorf("Sync was not called once, was called %d times", syncCalled)
	} else if syncDef == nil {
		t.Errorf("Sync was called with a nil syncDef")
	}

	// The emitted event has no service ids
	es, err := events.AllEvents(time.Time{}, -1, time.Time{})
	if err != nil {
		t.Error(err)
	} else if len(es) != 1 {
		t.Errorf("Unexpected events: %#v", es)
	} else if es[0].Type != event.EventSync {
		t.Errorf("Unexpected event type: %#v", es[0])
	} else {
		gotResourceIDs := es[0].ServiceIDs
		flux.ResourceIDs(gotResourceIDs).Sort()
		// Event should only have changed service ids
		if !reflect.DeepEqual(gotResourceIDs, []flux.ResourceID{flux.MustParseResourceID("default:deployment/helloworld")}) {
			t.Errorf("Unexpected event service ids: %#v, expected: %#v", gotResourceIDs, []flux.ResourceID{flux.MustParseResourceID("default:deployment/helloworld")})
		}
	}
	// It moves the tag
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := d.Repo.Refresh(ctx); err != nil {
		t.Errorf("pulling sync tag: %v", err)
	} else if revs, err := d.Repo.CommitsBetween(ctx, oldRevision, gitSyncTag); err != nil {
		t.Errorf("finding revisions before sync tag: %v", err)
	} else if len(revs) <= 0 {
		t.Errorf("Should have moved sync tag forward")
	} else if revs[len(revs)-1].Revision != newRevision {
		t.Errorf("Should have moved sync tag to HEAD (%s), but was moved to: %s", newRevision, revs[len(revs)-1].Revision)
	}
}
