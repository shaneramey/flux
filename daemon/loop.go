package daemon

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	fluxmetrics "github.com/weaveworks/flux/metrics"
)

type LoopVars struct {
	SyncInterval         time.Duration
	RegistryPollInterval time.Duration
	GitOpTimeout         time.Duration
	GitVerifySignatures  bool

	initOnce       sync.Once
	syncSoon       chan struct{}
	pollImagesSoon chan struct{}
	lastKnownSyncTagRev string
	warnedAboutSyncTagChange bool
	blockImagePoll bool
}

func (loop *LoopVars) ensureInit() {
	loop.initOnce.Do(func() {
		loop.syncSoon = make(chan struct{}, 1)
		loop.pollImagesSoon = make(chan struct{}, 1)
		loop.blockImagePoll = true
	})
}

func (d *Daemon) Loop(stop chan struct{}, wg *sync.WaitGroup, logger log.Logger) {
	defer wg.Done()

	// We want to sync at least every `SyncInterval`. Being told to
	// sync, or completing a job, may intervene (in which case,
	// reschedule the next sync).
	syncTimer := time.NewTimer(d.SyncInterval)
	// Similarly checking to see if any controllers have new images
	// available.
	imagePollTimer := time.NewTimer(d.RegistryPollInterval)

	// Keep track of current HEAD, so we can know when to treat a repo
	// mirror notification as a change. Otherwise, we'll just sync
	// every timer tick as well as every mirror refresh.
	syncHead := ""

	// Ask for a sync, and to poll images, straight away
	d.AskForSync()
	d.AskForImagePoll()

	for {
		select {
		case <-stop:
			logger.Log("stopping", "true")
			return
		case <-d.pollImagesSoon:
			if !imagePollTimer.Stop() {
				select {
				case <-imagePollTimer.C:
				default:
				}
			}
			d.pollForNewImages(logger)
			imagePollTimer.Reset(d.RegistryPollInterval)
		case <-imagePollTimer.C:
			d.AskForImagePoll()
		case <-d.syncSoon:
			if !syncTimer.Stop() {
				select {
				case <-syncTimer.C:
				default:
				}
			}
			sync := d.NewSync(logger)
			ctx := context.Background()
			if err := sync.Run(ctx); err != nil {
				logger.Log("err", err)
			}
			syncTimer.Reset(d.SyncInterval)
		case <-syncTimer.C:
			d.AskForSync()
		case <-d.Repo.C:
			ctx, cancel := context.WithTimeout(context.Background(), d.GitOpTimeout)
			newSyncHead, err := d.Repo.Revision(ctx, d.GitConfig.Branch)
			cancel()
			if err != nil {
				logger.Log("url", d.Repo.Origin().URL, "err", err)
				continue
			}
			logger.Log("event", "refreshed", "url", d.Repo.Origin().URL, "branch", d.GitConfig.Branch, "HEAD", newSyncHead)
			if newSyncHead != syncHead {
				syncHead = newSyncHead
				d.AskForSync()
			}
		case job := <-d.Jobs.Ready():
			queueLength.Set(float64(d.Jobs.Len()))
			jobLogger := log.With(logger, "jobID", job.ID)
			jobLogger.Log("state", "in-progress")
			// It's assumed that (successful) jobs will push commits
			// to the upstream repo, and therefore we probably want to
			// pull from there and sync the cluster afterwards.
			start := time.Now()
			err := job.Do(jobLogger)
			jobDuration.With(
				fluxmetrics.LabelSuccess, fmt.Sprint(err == nil),
			).Observe(time.Since(start).Seconds())
			if err != nil {
				jobLogger.Log("state", "done", "success", "false", "err", err)
			} else {
				jobLogger.Log("state", "done", "success", "true")
				ctx, cancel := context.WithTimeout(context.Background(), d.GitOpTimeout)
				err := d.Repo.Refresh(ctx)
				if err != nil {
					logger.Log("err", err)
				}
				cancel()
			}
		}
	}
}

// Ask for a sync, or if there's one waiting, let that happen.
func (d *LoopVars) AskForSync() {
	d.ensureInit()
	select {
	case d.syncSoon <- struct{}{}:
	default:
	}
}

// Ask for an image poll, or if there's one waiting, let that happen.
func (d *LoopVars) AskForImagePoll() {
	d.ensureInit()
	select {
	case d.pollImagesSoon <- struct{}{}:
	default:
	}
}
