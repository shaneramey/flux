package daemon

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/kit/log"

	"github.com/weaveworks/flux"
	"github.com/weaveworks/flux/cluster"
	"github.com/weaveworks/flux/cluster/kubernetes"
	"github.com/weaveworks/flux/cluster/kubernetes/testfiles"
	"github.com/weaveworks/flux/event"
	"github.com/weaveworks/flux/git"
	"github.com/weaveworks/flux/git/gittest"
)

const (
	gitPath     = ""
	gitSyncTag  = "flux-sync"
	gitNotesRef = "flux"
	gitUser     = "Weave Flux"
	gitEmail    = "support@weave.works"
)

var (
	k8s              *cluster.Mock
	events           *mockEventWriter
	syncDef          *cluster.SyncSet
	syncCalled       = 0
	defaultGitConfig = git.Config{
		Branch:    "master",
		SyncTag:   gitSyncTag,
		NotesRef:  gitNotesRef,
		UserName:  gitUser,
		UserEmail: gitEmail,
		Timeout:   10 * time.Second,
	}
)

func setupSync(t *testing.T, gitConfig git.Config) (*Sync, func()) {
	checkout, repo, cleanup := gittest.CheckoutWithConfig(t, gitConfig)

	k8s = &cluster.Mock{}
	k8s.ExportFunc = func() ([]byte, error) { return nil, nil }
	k8s.SyncFunc = func(def cluster.SyncSet) error {
		syncCalled++
		syncDef = &def
		return nil
	}

	events = &mockEventWriter{}

	if err := repo.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}

	s := &Sync{
		logger:      log.NewLogfmtLogger(os.Stdout),
		working:     checkout,
		repo:        repo,
		gitConfig:   gitConfig,
		manifests:   &kubernetes.Manifests{Namespacer: alwaysDefault},
		cluster:     k8s,
		eventLogger: events,
	}

	return s, func() {
		cleanup()
		syncCalled = 0
		syncDef = nil
		k8s = nil
		events = nil
	}
}

func TestRun_Initial(t *testing.T) {
	s, cleanup := setupSync(t, defaultGitConfig)
	defer cleanup()

	syncTag := lastKnownSyncTag{logger: s.logger, syncTag: s.gitConfig.SyncTag}
	if err := s.Run(context.Background(), &syncTag); err != nil {
		t.Fatal(err)
	}

	// It applies everything
	if syncCalled != 1 {
		t.Errorf("Sync was not called once, was called %d times", syncCalled)
	} else if syncDef == nil {
		t.Errorf("Sync was called with a nil syncDef")
	}

	// Collect expected resource IDs
	expectedResourceIDs := flux.ResourceIDs{}
	for id, _ := range testfiles.ResourceMap {
		expectedResourceIDs = append(expectedResourceIDs, id)
	}
	expectedResourceIDs.Sort()

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
	if err := s.repo.Refresh(context.Background()); err != nil {
		t.Errorf("Pulling sync tag: %v", err)
	} else if revs, err := s.repo.CommitsBefore(context.Background(), gitSyncTag); err != nil {
		t.Errorf("Finding revisions before sync tag: %v", err)
	} else if len(revs) <= 0 {
		t.Errorf("Found no revisions before the sync tag")
	}

	// It sets the last known tag
	if syncTag.Revision() == "" {
		t.Errorf("Expected last known revision to be set")
	}
}

func TestRun_NoNewCommit(t *testing.T) {
	s, cleanup := setupSync(t, defaultGitConfig)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), s.gitConfig.Timeout)
	tagAction := git.TagAction{
		Revision: "HEAD",
		Message:  "Sync pointer",
	}
	if err := s.working.MoveSyncTagAndPush(ctx, tagAction); err != nil {
		t.Fatal(err)
	}
	cancel()

	syncTag := lastKnownSyncTag{logger: s.logger, syncTag: s.gitConfig.SyncTag}
	if err := s.Run(context.Background(), &syncTag); err != nil {
		t.Fatal(err)
	}

	// It applies everything
	if syncCalled != 1 {
		t.Errorf("Sync was not called once, was called %d times", syncCalled)
	} else if syncDef == nil {
		t.Errorf("Sync was called with a nil syncDef")
	}

	// It doesn't update the last known tag revision
	if syncTag.Revision() != "" {
		t.Errorf("Expected last known revision to be empty")
	}
}

func TestRun_WithNewCommit(t *testing.T) {
	s, cleanup := setupSync(t, defaultGitConfig)
	defer cleanup()

	var err error
	var oldRevision, newRevision string
	ctx, cancel := context.WithTimeout(context.Background(), s.gitConfig.Timeout)
	defer cancel()

	// Create existing sync tag
	tagAction := git.TagAction{
		Revision: "HEAD",
		Message:  "Sync pointer",
	}
	if err = s.working.MoveSyncTagAndPush(ctx, tagAction); err != nil {
		t.Fatal(err)
	}
	if oldRevision, err = s.working.HeadRevision(ctx); err != nil {
		t.Fatal(err)
	}

	// Push new commit
	dirs := s.working.ManifestDirs()
	if err = cluster.UpdateManifest(s.manifests, s.working.Dir(), dirs, flux.MustParseResourceID("default:deployment/helloworld"), func(def []byte) ([]byte, error) {
		// A simple modification so we have changes to push
		return []byte(strings.Replace(string(def), "replicas: 5", "replicas: 4", -1)), nil
	}); err != nil {
		t.Fatal(err)
	}
	commitAction := git.CommitAction{Author: "", Message: "test commit"}
	err = s.working.CommitAndPush(ctx, commitAction, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Get expected revision
	newRevision, err = s.working.HeadRevision(ctx)
	if err = s.repo.Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	syncTag := lastKnownSyncTag{logger: s.logger, syncTag: s.gitConfig.SyncTag, revision: oldRevision}
	if err := s.Run(context.Background(), &syncTag); err != nil {
		t.Fatal(err)
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

	// It moves sync tag
	if err := s.repo.Refresh(ctx); err != nil {
		t.Errorf("Pulling sync tag: %v", err)
	} else if revs, err := s.repo.CommitsBetween(ctx, oldRevision, s.gitConfig.SyncTag); err != nil {
		t.Errorf("Finding revisions before sync tag: %v", err)
	} else if len(revs) <= 0 {
		t.Errorf("Should have moved sync tag forward")
	} else if revs[len(revs)-1].Revision != newRevision {
		t.Errorf("Should have moved sync tag to HEAD (%s), but was moved to: %s", newRevision, revs[len(revs)-1].Revision)
	}

	// It moves last known sync tag
	if syncTag.Revision() != newRevision {
		t.Errorf("Unexpected last known revision: %s, expected: %s", syncTag.Revision(), newRevision)
	}
}
