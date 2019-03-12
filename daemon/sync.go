package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	SetRevision(oldRev, NewRev string)
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

// NewSync initializes a new sync for the given revision.
func (d *Daemon) NewSync(logger log.Logger, revision string) (Sync, error) {
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
	if err != nil {
		return s, err
	}
	cancel()

	if headRev, err := s.working.HeadRevision(context.Background()); err != nil {
		return s, err
	} else if headRev != revision {
		err = s.working.Checkout(context.Background(), revision)
	}

	return s, err
}

// Run starts the synchronization of the cluster with git.
func (s *Sync) Run(ctx context.Context, synctag SyncTag) error {
	s.started = time.Now().UTC()
	defer s.working.Clean()

	c, err := getChangeset(ctx, s.working, s.repo, s.gitConfig)
	if err != nil {
		return err
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
		if err := moveSyncTag(ctx, s, c); err != nil {
			return err
		}
		synctag.SetRevision(c.oldTagRev, c.newTagRev)
		if err := refresh(ctx, s); err != nil {
			return err
		}
	}

	return nil
}

// getChangeset returns the changeset of commits for this sync,
// including the revision range and if it is an initial sync.
func getChangeset(ctx context.Context, working *git.Checkout, repo *git.Repo, gitConfig git.Config) (changeset, error) {
	var c changeset
	var err error

	c.oldTagRev, err = working.SyncRevision(ctx)
	if err != nil && !isUnknownRevision(err) {
		return c, err
	}
	c.newTagRev, err = working.HeadRevision(ctx)
	if err != nil {
		return c, err
	}

	ctx, cancel := context.WithTimeout(ctx, gitConfig.Timeout)
	if c.oldTagRev != "" {
		c.commits, err = repo.CommitsBetween(ctx, c.oldTagRev, c.newTagRev, gitConfig.Paths...)
	} else {
		c.initialSync = true
		c.commits, err = repo.CommitsBefore(ctx, c.newTagRev, gitConfig.Paths...)
	}
	cancel()

	return c, err
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
func moveSyncTag(ctx context.Context, s *Sync, c changeset) error {
	tagAction := git.TagAction{
		Revision: c.newTagRev,
		Message:  "Sync pointer",
	}
	ctx, cancel := context.WithTimeout(ctx, s.gitConfig.Timeout)
	if err := s.working.MoveSyncTagAndPush(ctx, tagAction); err != nil {
		return err
	}
	cancel()
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
