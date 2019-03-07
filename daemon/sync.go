package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"strings"
	"time"

	"github.com/weaveworks/flux"
	"github.com/weaveworks/flux/cluster"
	"github.com/weaveworks/flux/event"
	"github.com/weaveworks/flux/git"
	fluxmetrics "github.com/weaveworks/flux/metrics"
	"github.com/weaveworks/flux/resource"
	fluxsync "github.com/weaveworks/flux/sync"
	"github.com/weaveworks/flux/update"
)

// Sync holds the data we are working with during a sync.
type Sync struct {
	logger      log.Logger
	working     *git.Checkout
	oldTagRev   string
	newTagRev   string
	initialSync bool
	commits     []git.Commit

	*Daemon
}

// NewSync prepares a sync.
func (d *Daemon) NewSync(logger log.Logger) Sync {
	s := Sync{logger: logger, Daemon: d}

	return s
}

// Run starts the sync.
func (s *Sync) Run(ctx context.Context) (retErr error) {
	started := time.Now().UTC()
	defer func() {
		syncDuration.With(
			fluxmetrics.LabelSuccess, fmt.Sprint(retErr == nil),
		).Observe(time.Since(started).Seconds())
	}()

	if err := s.checkout(ctx); err != nil {
		return err
	}
	defer s.working.Clean()

	if err := s.loadTagRevisions(ctx); err != nil {
		return err
	}

	if s.GitVerifySignatures {
		if err := s.verifySyncTagSignature(ctx); err != nil {
			return err
		}
		if err := s.verifyCommitSignatures(); err != nil {
			s.logger.Log("err", err)
		}
		if err := s.verifyWorkingState(ctx); err != nil {
			return err
		}
	}

	syncSetName := makeSyncLabel(s.Repo.Origin(), s.GitConfig)
	resources, resourceErrors, err := s.doSync(syncSetName)
	if err != nil {
		return err
	}

	changedResources, err := s.getChangedResources(ctx, resources)
	serviceIDs := flux.ResourceIDSet{}
	for _, r := range changedResources {
		serviceIDs.Add([]flux.ResourceID{r.ResourceID()})
	}

	notes, err := s.getNotes(ctx)
	if err != nil {
		return err
	}
	noteEvents, includesEvents, err := s.collectNoteEvents(ctx, notes, started)
	if err != nil {
		return err
	}

	if err := s.logCommitEvent(serviceIDs, started, includesEvents, resourceErrors); err != nil {
		return err
	}

	for _, event := range noteEvents {
		if err = s.LogEvent(event); err != nil {
			s.logger.Log("err", err)
			// Abort early to ensure at least once delivery of events
			return err
		}
	}

	if s.newTagRev != s.oldTagRev {
		if err := s.moveSyncTag(ctx); err != nil {
			return err
		}
		s.logger.Log("tag", s.GitConfig.SyncTag, "old", s.oldTagRev, "new", s.newTagRev)
		if err := s.refresh(ctx); err != nil {
			return err
		}
	}

	return nil
}

// checkout checks out a working clone used for this sync.
func (s *Sync) checkout(ctx context.Context) error {
	var err error
	ctx, cancel := context.WithTimeout(ctx, s.GitOpTimeout)
	s.working, err = s.Repo.Clone(ctx, s.GitConfig)
	cancel()
	return err
}

// loadTagRevision retrieves the tag revision(s) for this sync.
func (s *Sync) loadTagRevisions(ctx context.Context) error {
	var err error
	s.oldTagRev, err = s.working.SyncRevision(ctx)
	if err != nil && !isUnknownRevision(err) {
		return err
	}

	// Check if something other than the current instance of fluxd changed the sync tag.
	// This is likely to be caused by another fluxd instance using the same tag.
	// Having multiple instances fighting for the same tag can lead to fluxd missing manifest changes.
	if s.lastKnownSyncTagRev != "" && s.oldTagRev != s.lastKnownSyncTagRev && !s.warnedAboutSyncTagChange {
		s.logger.Log("warning",
			"detected external change in git sync tag; the sync tag should not be shared by fluxd instances")
		s.warnedAboutSyncTagChange = true
	}

	s.newTagRev, err = s.working.HeadRevision(ctx)
	return err
}

// loadCommitChangeset retrieves the commit changeset for this sync.
func (s *Sync) loadCommitChangeset(ctx context.Context) error {
	var err error
	ctx, cancel := context.WithTimeout(ctx, s.GitOpTimeout)
	if s.oldTagRev != "" {
		s.commits, err = s.Repo.CommitsBetween(ctx, s.oldTagRev, s.newTagRev, s.GitConfig.Paths...)
	} else {
		s.initialSync = true
		s.commits, err = s.Repo.CommitsBefore(ctx, s.newTagRev, s.GitConfig.Paths...)
	}
	cancel()
	return err
}

// verifySyncTagSignature verifies the signature of the current sync
// tag, if verification fails _while we are not doing an initial sync_
// it blocks image polling and returns an error.
func (s *Sync) verifySyncTagSignature(ctx context.Context) error {
	err := s.working.VerifySyncTag(ctx)
	if !s.initialSync && err != nil {
		s.blockImagePolling(true)
		return errors.Wrap(err, "failed to verify signature of sync tag")
	}
	return nil
}

// verifyCommitSignatures verifies the signatures of the commits we are
// working with, it does so by looping through the commits in ascending
// order and requesting the validity of each signature. In case of
// failure it blocks image polling, mutates the set of the commits we
// are working with to the ones we have validated and returns an error.
func (s *Sync) verifyCommitSignatures() error {
	for i := len(s.commits) - 1; i >= 0; i-- {
		if !s.commits[i].Signature.Valid() {
			s.blockImagePolling(true)
			s.commits = s.commits[i+1:]
			return fmt.Errorf("invalid GPG signature for commit %s with key %s", s.commits[i].Revision, s.commits[i].Signature.Key)
		}
	}
	return nil
}

// verifyWorkingState verifies if the state of the working git
// repository and newTagRev is still equal to the commit changeset we
// have. This is required when working with GPG signatures as we may
// be working with a mutated commit changeset due to commit signature
// verification errors.
func (s *Sync) verifyWorkingState(ctx context.Context) error {
	// We have no valid commits, determine what we should do next...
	if len(s.commits) == 0 {
		// We have no state to reapply either; abort...
		if s.initialSync {
			return errors.New("unable to sync as no commits with valid GPG signatures were found")
		}
		// Reapply the old rev as this is the latest valid state we saw
		s.newTagRev = s.oldTagRev
		return nil
	}

	// Check if the first commit in the slice still equals the
	// newTagRev. If this is not the case we need to checkout
	// the working clone to the revision of the commit from the
	// slice as otherwise we will be (re)applying unverified
	// state on the cluster.
	if latestCommitRev := s.commits[len(s.commits)-1].Revision; s.newTagRev != latestCommitRev {
		if err := s.working.Checkout(ctx, latestCommitRev); err != nil {
			return err
		}
		s.newTagRev = latestCommitRev
		return nil
	}
	// All verification succeeded. Make sure we are no longer block
	// image polling.
	s.blockImagePolling(false)
	return nil
}

// doSync runs the actual sync of workloads on the cluster. It returns
// a map with all resources it applied and sync errors it encountered.
func (s *Sync) doSync(syncSetName string) (map[string]resource.Resource, []event.ResourceError, error) {
	resources, err := s.Manifests.LoadManifests(s.working.Dir(), s.working.ManifestDirs())
	if err != nil {
		return nil, nil, errors.Wrap(err, "loading resources from repo")
	}

	var resourceErrors []event.ResourceError
	if err := fluxsync.Sync(syncSetName, resources, s.Cluster); err != nil {
		s.logger.Log("err", err)
		switch syncerr := err.(type) {
		case cluster.SyncError:
			for _, e := range syncerr {
				resourceErrors = append(resourceErrors, event.ResourceError{
					ID:    e.ResourceID,
					Path:  e.Source,
					Error: e.Error.Error(),
				})
			}
		default:
			return nil, nil, err
		}
	}
	return resources, resourceErrors, nil
}

// getChangedResources returns what resources are modified during this
// sync.
func (s *Sync) getChangedResources(ctx context.Context, resources map[string]resource.Resource) (map[string]resource.Resource, error) {
	if s.initialSync {
		return resources, nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.GitOpTimeout)
	changedFiles, err := s.working.ChangedFiles(ctx, s.oldTagRev)
	if err == nil && len(changedFiles) > 0 {
		// We had some changed files, we're syncing a diff
		// FIXME(michael): this won't be accurate when a file can have more than one resource
		resources, err = s.Manifests.LoadManifests(s.working.Dir(), changedFiles)
	}
	cancel()
	if err != nil {
		return nil, errors.Wrap(err, "loading resources from repo")
	}
	return resources, nil
}

// getNotes retrieves the git notes from the working clone.
func (s *Sync) getNotes(ctx context.Context) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, s.GitOpTimeout)
	notes, err := s.working.NoteRevList(ctx)
	cancel()
	if err != nil {
		return nil, errors.Wrap(err, "loading notes from repo")
	}
	return notes, nil
}

// collectNoteEvents collects any events that come from notes attached
// to the commits we just synced. While we're doing this, keep track
// of what other things this sync includes e.g., releases and
// autoreleases, that we're already posting as events, so upstream
// can skip the sync event if it wants to.
func (s *Sync) collectNoteEvents(ctx context.Context, notes map[string]struct{}, started time.Time) ([]event.Event, map[string]bool, error) {
	if len(s.commits) == 0 {
		return nil, nil, nil
	}

	var noteEvents []event.Event
	var eventTypes map[string]bool

	// Find notes in revisions.
	for i := len(s.commits) - 1; i >= 0; i-- {
		if _, ok := notes[s.commits[i].Revision]; !ok {
			eventTypes[event.NoneOfTheAbove] = true
			continue
		}
		ctx, cancel := context.WithTimeout(ctx, s.GitOpTimeout)
		var n note
		ok, err := s.working.GetNote(ctx, s.commits[i].Revision, &n)
		cancel()
		if err != nil {
			return nil, nil, errors.Wrap(err, "loading notes from repo")
		}
		if !ok {
			eventTypes[event.NoneOfTheAbove] = true
			continue
		}

		// If this is the first sync, we should expect no notes,
		// since this is supposedly the first time we're seeing
		// the repo. But there are circumstances in which we can
		// nonetheless see notes -- if the tag was deleted from
		// the upstream repo, or if this accidentally has the same
		// notes ref as another daemon using the same repo (but a
		// different tag). Either way, we don't want to report any
		// notes on an initial sync, since they (most likely)
		// don't belong to us.
		if s.initialSync {
			s.logger.Log("warning", "no notes expected on initial sync; this repo may be in use by another fluxd")
			return noteEvents, eventTypes, nil
		}

		// Interpret some notes as events to send to the upstream
		switch n.Spec.Type {
		case update.Containers:
			spec := n.Spec.Spec.(update.ReleaseContainersSpec)
			noteEvents = append(noteEvents, event.Event{
				ServiceIDs: n.Result.AffectedResources(),
				Type:       event.EventRelease,
				StartedAt:  started,
				EndedAt:    time.Now().UTC(),
				LogLevel:   event.LogLevelInfo,
				Metadata: &event.ReleaseEventMetadata{
					ReleaseEventCommon: event.ReleaseEventCommon{
						Revision: s.commits[i].Revision,
						Result:   n.Result,
						Error:    n.Result.Error(),
					},
					Spec: event.ReleaseSpec{
						Type:                  event.ReleaseContainersSpecType,
						ReleaseContainersSpec: &spec,
					},
					Cause: n.Spec.Cause,
				},
			})
			eventTypes[event.EventRelease] = true
		case update.Images:
			spec := n.Spec.Spec.(update.ReleaseImageSpec)
			noteEvents = append(noteEvents, event.Event{
				ServiceIDs: n.Result.AffectedResources(),
				Type:       event.EventRelease,
				StartedAt:  started,
				EndedAt:    time.Now().UTC(),
				LogLevel:   event.LogLevelInfo,
				Metadata: &event.ReleaseEventMetadata{
					ReleaseEventCommon: event.ReleaseEventCommon{
						Revision: s.commits[i].Revision,
						Result:   n.Result,
						Error:    n.Result.Error(),
					},
					Spec: event.ReleaseSpec{
						Type:             event.ReleaseImageSpecType,
						ReleaseImageSpec: &spec,
					},
					Cause: n.Spec.Cause,
				},
			})
			eventTypes[event.EventRelease] = true
		case update.Auto:
			spec := n.Spec.Spec.(update.Automated)
			noteEvents = append(noteEvents, event.Event{
				ServiceIDs: n.Result.AffectedResources(),
				Type:       event.EventAutoRelease,
				StartedAt:  started,
				EndedAt:    time.Now().UTC(),
				LogLevel:   event.LogLevelInfo,
				Metadata: &event.AutoReleaseEventMetadata{
					ReleaseEventCommon: event.ReleaseEventCommon{
						Revision: s.commits[i].Revision,
						Result:   n.Result,
						Error:    n.Result.Error(),
					},
					Spec: spec,
				},
			})
			eventTypes[event.EventAutoRelease] = true
		case update.Policy:
			// Use this to mean any change to policy
			eventTypes[event.EventUpdatePolicy] = true
		default:
			// Presume it's not something we're otherwise sending
			// as an event
			eventTypes[event.NoneOfTheAbove] = true
		}
	}
	return noteEvents, eventTypes, nil
}

// logCommitEvent reports all synced commits to the upstream.
func (s *Sync) logCommitEvent(serviceIDs flux.ResourceIDSet, started time.Time,
	includesEvents map[string]bool, resourceErrors []event.ResourceError) error {
	cs := make([]event.Commit, len(s.commits))
	for i, c := range s.commits {
		cs[i].Revision = c.Revision
		cs[i].Message = c.Message
	}
	if err := s.LogEvent(event.Event{
		ServiceIDs: serviceIDs.ToSlice(),
		Type:       event.EventSync,
		StartedAt:  started,
		EndedAt:    started,
		LogLevel:   event.LogLevelInfo,
		Metadata: &event.SyncEventMetadata{
			Commits:     cs,
			InitialSync: s.initialSync,
			Includes:    includesEvents,
			Errors:      resourceErrors,
		},
	}); err != nil {
		s.logger.Log("err", err)
		return err
	}
	return nil
}

// moveSyncTag moves the sync tag to the revision we just synced.
func (s *Sync) moveSyncTag(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.GitOpTimeout)
	tagAction := git.TagAction{
		Revision: s.newTagRev,
		Message:  "Sync pointer",
	}
	err := s.working.MoveSyncTagAndPush(ctx, tagAction)
	cancel()
	if err != nil {
		return err
	}
	s.lastKnownSyncTagRev = s.newTagRev
	return nil
}

// refresh refreshes the repository, notifying the daemon we have a new
// sync head.
func (s *Sync) refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.GitOpTimeout)
	err := s.Repo.Refresh(ctx)
	cancel()
	return err
}

// Blocks image updates, this should happen when an invalid signature
// was found because of two reasons:
// 1. we would apply updates on top of something we do not see as
//    valid, _indirectly_ giving it more authenticity
// 2. the pushed updates would never reach the cluster as we will
//    never get past the invalid commit we just encountered
func (s *Sync) blockImagePolling(block bool) {
	s.blockImagePoll = block
}

func isUnknownRevision(err error) bool {
	return err != nil &&
		(strings.Contains(err.Error(), "unknown revision or path not in the working tree.") ||
			strings.Contains(err.Error(), "bad revision"))
}

func makeSyncLabel(remote git.Remote, conf git.Config) string {
	urlbit := remote.SafeURL()
	pathshash := sha256.New()
	pathshash.Write([]byte(urlbit))
	pathshash.Write([]byte(conf.Branch))
	for _, path := range conf.Paths {
		pathshash.Write([]byte(path))
	}
	// the prefix is in part to make sure it's a valid (Kubernetes)
	// label value -- a modest abstraction leak
	return "git-" + base64.RawURLEncoding.EncodeToString(pathshash.Sum(nil))
}
