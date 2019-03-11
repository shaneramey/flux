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
	"github.com/weaveworks/flux/resource"
	fluxsync "github.com/weaveworks/flux/sync"
	"github.com/weaveworks/flux/update"
)

// Sync holds the data we are working with during a sync.
type Sync struct {
	started   time.Time
	logger    log.Logger
	working   *git.Checkout
	repo      *git.Repo
	gitConfig git.Config
	manifests cluster.Manifests
	cluster   cluster.Cluster
	eventLogger
}

type SyncTag interface {
	Revision() string
	SetRevision(rev string)
	WarnedAboutChange() bool
	SetWarnedAboutChange(warned bool)
}

type eventLogger interface {
	LogEvent(e event.Event) error
}

type changeset struct {
	commits     []git.Commit
	oldTagRev   string
	newTagRev   string
	initialSync bool
}

// NewSync initializes a new sync.
func (d *Daemon) NewSync(logger log.Logger) (Sync, error) {
	s := Sync{
		logger:      logger,
		repo:        d.Repo,
		gitConfig:   d.GitConfig,
		manifests:   d.Manifests,
		cluster:     d.Cluster,
		eventLogger: d,
	}

	// checkout out a working clone used for this sync.
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), s.gitConfig.Timeout)
	s.working, err = s.repo.Clone(ctx, s.gitConfig)
	cancel()

	return s, err
}

// Run starts the synchronization of the cluster with git.
func (s *Sync) Run(ctx context.Context, synctag SyncTag, imagePollLock ImagePollLock) error {
	s.started = time.Now().UTC()
	defer s.working.Clean()

	c, err := getChangeset(ctx, s)
	if err != nil {
		return err
	}

	// Check if something other than the current instance of fluxd changed the sync tag.
	// This is likely to be caused by another fluxd instance using the same tag.
	// Having multiple instances fighting for the same tag can lead to fluxd missing manifest changes.
	if synctag.Revision() != "" && c.oldTagRev != synctag.Revision() && !synctag.WarnedAboutChange() {
		s.logger.Log("warning",
			"detected external change in git sync tag; the sync tag should not be shared by fluxd instances")
		synctag.SetWarnedAboutChange(true)
	}

	if s.gitConfig.VerifySignatures {
		if err := verifySyncTagSignature(ctx, s.working, c); err != nil {
			imagePollLock.Lock(true)
			return err
		}
		if err = verifyCommitSignatures(&c); err != nil {
			imagePollLock.Lock(true)
			s.logger.Log("err", err)
		}
		ok, err := verifyWorkingState(ctx, s.working, &c)
		imagePollLock.Lock(!ok)
		if err != nil {
			return err
		}
	}

	syncSetName := makeSyncLabel(s.repo.Origin(), s.gitConfig)
	resources, resourceErrors, err := doSync(s, syncSetName)
	if err != nil {
		return err
	}

	changedResources, err := getChangedResources(ctx, s, c, resources)
	serviceIDs := flux.ResourceIDSet{}
	for _, r := range changedResources {
		serviceIDs.Add([]flux.ResourceID{r.ResourceID()})
	}

	notes, err := getNotes(ctx, s)
	if err != nil {
		return err
	}
	noteEvents, includesEvents, err := collectNoteEvents(ctx, s, c, notes)
	if err != nil {
		return err
	}

	if err := logCommitEvent(s, c, serviceIDs, includesEvents, resourceErrors); err != nil {
		return err
	}

	for _, event := range noteEvents {
		if err = s.LogEvent(event); err != nil {
			s.logger.Log("err", err)
			// Abort early to ensure at least once delivery of events
			return err
		}
	}

	if c.newTagRev != c.oldTagRev {
		if err := moveSyncTag(ctx, s, c, synctag); err != nil {
			return err
		}
		s.logger.Log("tag", s.gitConfig.SyncTag, "old", c.oldTagRev, "new", c.newTagRev)
		if err := refresh(ctx, s); err != nil {
			return err
		}
	}

	return nil
}

// getChangeset returns the changeset of commits for this sync,
// including the revision range and if it is an initial sync.
func getChangeset(ctx context.Context, s *Sync) (changeset, error) {
	var c changeset
	var err error

	c.oldTagRev, err = s.working.SyncRevision(ctx)
	if err != nil && !isUnknownRevision(err) {
		return c, err
	}
	c.newTagRev, err = s.working.HeadRevision(ctx)
	if err != nil {
		return c, err
	}

	ctx, cancel := context.WithTimeout(ctx, s.gitConfig.Timeout)
	if c.oldTagRev != "" {
		c.commits, err = s.repo.CommitsBetween(ctx, c.oldTagRev, c.newTagRev, s.gitConfig.Paths...)
	} else {
		c.initialSync = true
		c.commits, err = s.repo.CommitsBefore(ctx, c.newTagRev, s.gitConfig.Paths...)
	}
	cancel()

	return c, err
}

// verifySyncTagSignature verifies the signature of the current sync
// tag, if verification fails _while we are not doing an initial sync_
// it returns an error.
func verifySyncTagSignature(ctx context.Context, working *git.Checkout, c changeset) error {
	err := working.VerifySyncTag(ctx)
	if !c.initialSync && err != nil {
		return errors.Wrap(err, "failed to verify signature of sync tag")
	}
	return nil
}

// verifyCommitSignatures verifies the signatures of the commits we are
// working with, it does so by looping through the commits in ascending
// order and requesting the validity of each signature. In case of
// failure it mutates the set of the changeset of commits we are
// working with to the ones we have validated and returns an error.
func verifyCommitSignatures(c *changeset) error {
	for i := len(c.commits) - 1; i >= 0; i-- {
		if !c.commits[i].Signature.Valid() {
			err := fmt.Errorf(
				"invalid GPG signature for commit %s with key %s",
				c.commits[i].Revision,
				c.commits[i].Signature.Key,
			)
			c.commits = c.commits[i+1:]
			return err
		}
	}
	return nil
}

// verifyWorkingState verifies if the state of the working git
// repository and newTagRev is still equal to the commit changeset we
// have. This is required when working with GPG signatures as we may
// be working with a mutated commit changeset due to commit signature
// verification errors. It returns true if the state is secure to work
// with.
func verifyWorkingState(ctx context.Context, working *git.Checkout, c *changeset) (bool, error) {
	// We have no valid commits, determine what we should do next...
	if len(c.commits) == 0 {
		// We have no state to reapply either; abort...
		if c.initialSync {
			return false, errors.New("unable to sync as no commits with valid GPG signatures were found")
		}
		// Reapply the old rev as this is the latest valid state we saw
		c.newTagRev = c.oldTagRev
		return false, nil
	}

	// Check if the first commit in the slice still equals the
	// newTagRev. If this is not the case we need to checkout
	// the working clone to the revision of the commit from the
	// slice as otherwise we will be (re)applying unverified
	// state on the cluster.
	if latestCommitRev := c.commits[len(c.commits)-1].Revision; c.newTagRev != latestCommitRev {
		if err := working.Checkout(ctx, latestCommitRev); err != nil {
			return false, err
		}
		c.newTagRev = latestCommitRev
		return false, nil
	}
	return true, nil
}

// doSync runs the actual sync of workloads on the cluster. It returns
// a map with all resources it applied and sync errors it encountered.
func doSync(s *Sync, syncSetName string) (map[string]resource.Resource, []event.ResourceError, error) {
	resources, err := s.manifests.LoadManifests(s.working.Dir(), s.working.ManifestDirs())
	if err != nil {
		return nil, nil, errors.Wrap(err, "loading resources from repo")
	}

	var resourceErrors []event.ResourceError
	if err := fluxsync.Sync(syncSetName, resources, s.cluster); err != nil {
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

// getChangedResources calculates what resources are modified during
// this sync.
func getChangedResources(ctx context.Context, s *Sync, c changeset, resources map[string]resource.Resource) (map[string]resource.Resource, error) {
	if c.initialSync {
		return resources, nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.gitConfig.Timeout)
	changedFiles, err := s.working.ChangedFiles(ctx, c.oldTagRev)
	if err == nil && len(changedFiles) > 0 {
		// We had some changed files, we're syncing a diff
		// FIXME(michael): this won't be accurate when a file can have more than one resource
		resources, err = s.manifests.LoadManifests(s.working.Dir(), changedFiles)
	}
	cancel()
	if err != nil {
		return nil, errors.Wrap(err, "loading resources from repo")
	}
	return resources, nil
}

// getNotes retrieves the git notes from the working clone.
func getNotes(ctx context.Context, s *Sync) (map[string]struct{}, error) {
	ctx, cancel := context.WithTimeout(ctx, s.gitConfig.Timeout)
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
func collectNoteEvents(ctx context.Context, s *Sync, c changeset, notes map[string]struct{}) ([]event.Event, map[string]bool, error) {
	if len(c.commits) == 0 {
		return nil, nil, nil
	}

	var noteEvents []event.Event
	var eventTypes = make(map[string]bool)

	// Find notes in revisions.
	for i := len(c.commits) - 1; i >= 0; i-- {
		if _, ok := notes[c.commits[i].Revision]; !ok {
			eventTypes[event.NoneOfTheAbove] = true
			continue
		}
		var n note
		ctx, cancel := context.WithTimeout(ctx, s.gitConfig.Timeout)
		ok, err := s.working.GetNote(ctx, c.commits[i].Revision, &n)
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
		if c.initialSync {
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
				StartedAt:  s.started,
				EndedAt:    time.Now().UTC(),
				LogLevel:   event.LogLevelInfo,
				Metadata: &event.ReleaseEventMetadata{
					ReleaseEventCommon: event.ReleaseEventCommon{
						Revision: c.commits[i].Revision,
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
				StartedAt:  s.started,
				EndedAt:    time.Now().UTC(),
				LogLevel:   event.LogLevelInfo,
				Metadata: &event.ReleaseEventMetadata{
					ReleaseEventCommon: event.ReleaseEventCommon{
						Revision: c.commits[i].Revision,
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
				StartedAt:  s.started,
				EndedAt:    time.Now().UTC(),
				LogLevel:   event.LogLevelInfo,
				Metadata: &event.AutoReleaseEventMetadata{
					ReleaseEventCommon: event.ReleaseEventCommon{
						Revision: c.commits[i].Revision,
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
func logCommitEvent(s *Sync, c changeset, serviceIDs flux.ResourceIDSet,
	includesEvents map[string]bool, resourceErrors []event.ResourceError) error {
	cs := make([]event.Commit, len(c.commits))
	for i, ci := range c.commits {
		cs[i].Revision = ci.Revision
		cs[i].Message = ci.Message
	}
	if err := s.LogEvent(event.Event{
		ServiceIDs: serviceIDs.ToSlice(),
		Type:       event.EventSync,
		StartedAt:  s.started,
		EndedAt:    s.started,
		LogLevel:   event.LogLevelInfo,
		Metadata: &event.SyncEventMetadata{
			Commits:     cs,
			InitialSync: c.initialSync,
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
func moveSyncTag(ctx context.Context, s *Sync, c changeset, synctag SyncTag) error {
	tagAction := git.TagAction{
		Revision: c.newTagRev,
		Message:  "Sync pointer",
	}
	ctx, cancel := context.WithTimeout(ctx, s.gitConfig.Timeout)
	if err := s.working.MoveSyncTagAndPush(ctx, tagAction); err != nil {
		return err
	}
	cancel()
	synctag.SetRevision(c.newTagRev)
	return nil
}

// refresh refreshes the repository, notifying the daemon we have a new
// sync head.
func refresh(ctx context.Context, s *Sync) error {
	ctx, cancel := context.WithTimeout(ctx, s.gitConfig.Timeout)
	err := s.repo.Refresh(ctx)
	cancel()
	return err
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
