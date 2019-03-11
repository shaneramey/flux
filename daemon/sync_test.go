package daemon

import (
	"context"
	"github.com/weaveworks/flux/gpg/gpgtest"
	"os"
	"reflect"
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

func setupSync(t *testing.T, gitConfig git.Config, signCommits, verifySignatures bool) (*Sync, func()) {
	var gpgHome, gpgKey string
	var gpgCleanup = func() {}
	if signCommits {
		gpgHome, gpgKey, gpgCleanup = gpgtest.GPGKey(t)

		os.Setenv("GNUPGHOME", gpgHome)
		gitConfig.SigningKey = gpgKey
	}

	gitConfig.VerifySignatures = verifySignatures

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
		s.working.Clean() // we may be working with an alternative working clone
		cleanup()
		gpgCleanup()
		os.Unsetenv("GNUPGHOME")
		syncCalled = 0
		syncDef = nil
		k8s = nil
		events = nil
	}
}

func TestRun_InitialSync(t *testing.T) {
	testCases := map[string]struct {
		gitConfig         git.Config
		signCommits       bool
		verifySignatures  bool
		seedInvalidCommit bool

		expectRunError      bool
		expectImagePollLock bool
		expectSyncCalled    bool
		expectSyncTagChange bool
	}{
		"default": {defaultGitConfig, false, false, false, false, false, true, true},
		"signed commits without signature verification":     {defaultGitConfig, true, false, false, false, false, true, true},
		"signed commits with signature verification":        {defaultGitConfig, true, true, false, false, false, true, true},
		"unsigned commits with signature verification":      {defaultGitConfig, false, true, false, true, true, false, false},
		"invalid signed commit with signature verification": {defaultGitConfig, true, true, true, false, true, true, true},
	}
	for name, tc := range testCases {
		s, cleanup := setupSync(t, tc.gitConfig, tc.signCommits, tc.verifySignatures)

		ctx := context.Background()
		expectedRevision, _ := s.working.HeadRevision(ctx)

		if tc.seedInvalidCommit {
			if err := func() error {
				ctx, cancel := context.WithTimeout(ctx, s.gitConfig.Timeout)
				defer cancel()

				// Create temp checkout to make modifications
				tmpCheckout, err := s.repo.Clone(ctx, s.gitConfig)
				defer tmpCheckout.Clean()
				if err != nil {
					return err
				}

				// Make modification to file
				dirs := tmpCheckout.ManifestDirs()
				err = cluster.UpdateManifest(s.manifests, tmpCheckout.Dir(), dirs, flux.MustParseResourceID("default:deployment/helloworld"), func(def []byte) ([]byte, error) {
					// Empty the file, as we later verify if all
					// resource IDs are applied this should throw an
					// error if something is wrong.
					return []byte(""), nil
				})
				if err != nil {
					return err
				}

				// Create temp gpg signing key
				orgGpgHome := os.Getenv("GNUPGHOME")
				tmpGpgHome, tmpGpgKey, tmpGpgCleanup := gpgtest.GPGKey(t)
				defer tmpGpgCleanup()
				os.Setenv("GNUPGHOME", tmpGpgHome)
				defer os.Setenv("GNUPGHOME", orgGpgHome)

				// Commit change
				commitAction := git.CommitAction{Author: "", Message: "unknown signature commit", SigningKey: tmpGpgKey}
				err = tmpCheckout.CommitAndPush(ctx, commitAction, nil)
				return err
			}(); err != nil {
				cleanup()
				t.Fatalf("%s: failed to seed commit with invalid signature: %v", name, err)
			}

			if err := s.repo.Refresh(ctx); err != nil {
				cleanup()
				t.Fatalf("%s: failed to refresh: %v", name, err)
			}

			var err error
			s.working, err = s.repo.Clone(context.Background(), s.gitConfig)
			if err != nil {
				cleanup()
				t.Fatalf("%s: failed to clone new working repo: %v", name, err)
			}
		}

		syncTag := lastKnownSyncTag{}
		lock := imagePollLock{}

		// It (does not) return(s) err on run
		err := s.Run(context.Background(), &syncTag, &lock)
		if tc.expectRunError && err == nil {
			t.Errorf("%s: expected err on run but did not get one", name)
		} else if !tc.expectRunError && err != nil {
			t.Errorf("%s: expected no err on run but got: %v", name, err)
		}

		// It (does not) lock(s) image polling
		if lock.Locked() != tc.expectImagePollLock {
			t.Errorf("%s: expected lock to be %t but was %t", name, tc.expectImagePollLock, lock.Locked())
		}

		// It (does not) sync(s) to the cluster
		expectedSyncCalls := 0
		if tc.expectSyncCalled {
			expectedSyncCalls = 1
		}
		if syncCalled != expectedSyncCalls {
			t.Errorf("%s: sync was not called once, was called %d times", name, syncCalled)

			if syncDef == nil {
				t.Errorf("%s: sync was called with a nil syncDef", name)
			}
		}

		// Collect expected resource IDs
		expectedResourceIDs := flux.ResourceIDs{}
		for id, _ := range testfiles.ResourceMap {
			expectedResourceIDs = append(expectedResourceIDs, id)
		}
		expectedResourceIDs.Sort()

		// It sends out the correct event
		es, err := events.AllEvents(time.Time{}, -1, time.Time{})
		if err != nil {
			t.Errorf("%s: %v", name, err)
		} else if !tc.expectSyncCalled && len(es) >= 1 {
			t.Errorf("%s: expected no events got %d events", name, len(es))
		} else if tc.expectSyncCalled && len(es) != 1 {
			t.Errorf("%s: unexpected events: %#v", name, es)
		} else if tc.expectSyncCalled && es[0].Type != event.EventSync {
			t.Errorf("%s: unexpected event type: %#v", name, es[0])
		} else if tc.expectSyncCalled {
			gotResourceIDs := es[0].ServiceIDs
			flux.ResourceIDs(gotResourceIDs).Sort()
			if !reflect.DeepEqual(gotResourceIDs, []flux.ResourceID(expectedResourceIDs)) {
				t.Errorf("%s: unexpected event service ids: %#v, expected: %#v", name, gotResourceIDs, expectedResourceIDs)
			}
		}

		// It creates the correct tag
		var actualRevision string
		if err := s.repo.Refresh(ctx); err != nil {
			t.Errorf("%s: pulling sync tag: %v", name, err)
		} else if actualRevision, err = s.repo.Revision(ctx, s.gitConfig.SyncTag); tc.expectSyncTagChange && err != nil {
			t.Errorf("%s: finding revision for sync tag: %v", name, err)
		} else if tc.expectSyncTagChange && actualRevision != expectedRevision {
			t.Errorf("%s: expected sync tag revision to be: %s, got: %s", name, expectedRevision, actualRevision)
		} else if tc.expectSyncTagChange && syncTag.Revision() != expectedRevision {
			t.Errorf("%s: expected last known sync tag revision to be: %s, got: %s", name, expectedRevision, syncTag.Revision())
		} else if !tc.expectSyncTagChange && actualRevision != "" {
			t.Errorf("%s: expected no sync tag revision, got: %s", name, actualRevision)
		}

		cleanup()
	}
}
