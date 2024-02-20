package worker

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/cloudberrydb/cbload/log"
	"github.com/jackc/pgconn"
)

type Worker struct {
	context           context.Context
	cancelContextFunc context.CancelFunc
	conn              *pgconn.PgConn
	id                int
	localFiles        []string
	destFiles         []string
	manager           *WorkManager
}

type WorkManager struct {
	sync.Mutex
	waitGroup      sync.WaitGroup
	table          string
	tag            string
	conns          []*pgconn.PgConn
	localFiles     [][]string
	destFiles      [][]string
	workers        []*Worker
	doneCh         chan struct{}
	quickDieCh     chan struct{}
	syncCh         chan struct{}
	numFileSucceed atomic.Uint32
	numFileFailed  atomic.Uint32
	stopOnError    bool
}

func NewWorkManager(table, tag string, conns []*pgconn.PgConn, localFiles, destFiles [][]string, stopOnError bool) *WorkManager {
	wm := &WorkManager{
		table:       table,
		tag:         tag,
		conns:       conns,
		localFiles:  localFiles,
		destFiles:   destFiles,
		stopOnError: stopOnError,
		workers:     make([]*Worker, len(conns)),
		doneCh:      make(chan struct{}, 1),
		quickDieCh:  make(chan struct{}, 1),
		syncCh:      make(chan struct{}, 1),
	}
	return wm
}

func (wm *WorkManager) Start() (chan struct{}, chan struct{}) {
	log.Logger.Debug("WorkManager starting...")
	numWorkers := len(wm.conns)

	wm.waitGroup.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		wm.workers[i] = newWorker(wm, i, wm.conns[i], wm.localFiles[i], wm.destFiles[i])
		go wm.workers[i].Run()
	}

	go func() {
		wm.waitGroup.Wait()
		log.Logger.Debug("All workers have completed")
		wm.syncCh <- struct{}{}
		wm.doneCh <- struct{}{}
		log.Logger.Debug("The main thread has already been notified")
	}()

	return wm.doneCh, wm.quickDieCh
}

func (wm *WorkManager) GetNumSucceedFiles() uint32 {
	return wm.numFileSucceed.Load()
}

func (wm *WorkManager) GetNumFailedFiles() uint32 {
	return wm.numFileFailed.Load()
}

func (wm *WorkManager) Stop() {
	log.Logger.Debug("WorkManager stopping...")

	numWorkers := len(wm.conns)
	for i := 0; i < numWorkers; i++ {
		wm.workers[i].Stop()
	}

	<-wm.syncCh
	log.Logger.Debug("WorkManager stopped")
}

func newWorker(manager *WorkManager, id int, conn *pgconn.PgConn, localFiles, destFiles []string) *Worker {
	ctx, cancelFunc := context.WithCancel(context.Background())
	worker := &Worker{ctx, cancelFunc, conn, id, localFiles, destFiles, manager}
	return worker
}

func (w *Worker) Run() {
	defer w.manager.waitGroup.Done()

	log.Logger.Debugf("Worker [%v]: started", w.id)

loop:
	for i, file := range w.localFiles {
		fh, err := os.Open(file)
		if err != nil {
			if w.handleError("unable to open file", file, err) {
				break
			}
			continue
		}

		log.Logger.Infof("Worker [%v]: loading file \"%v\" into \"%v:%v\"...", w.id, file, w.manager.table, w.destFiles[i])

		_, err = w.conn.CopyFrom(w.context,
			bufio.NewReader(fh),
			w.formCopyStatement(w.destFiles[i]))
		if err != nil {
			if w.handleError("unable to upload file", file, err) {
				break
			}
			continue
		}

		w.manager.numFileSucceed.Add(1)
		fh.Close()
		log.Logger.Infof("Worker [%v]: successfully loaded", w.id)

		select {
		case <-w.context.Done():
			break loop
		default:
		}
	}

	log.Logger.Debugf("Worker [%v]: stopped", w.id)
}

func (w *Worker) handleError(title, file string, err error) bool {
	w.manager.numFileFailed.Add(1)
	log.Logger.Errorf("Worker [%v]: %v \"%v\": %v", w.id, title, file, err)
	if w.manager.stopOnError {
		w.manager.quickDieCh <- struct{}{}
		return true
	}

	return false
}

func (w *Worker) formCopyStatement(file string) string {
	if len(w.manager.tag) == 0 {
		return fmt.Sprintf("copy binary %s from stdin '%s';", w.manager.table, file)
	}

	return fmt.Sprintf("copy binary %s from stdin '%s' with tag '%s';", w.manager.table, file, w.manager.tag)
}

func (w *Worker) Stop() {
	log.Logger.Debugf("Worker [%v]: stopping...", w.id)
	w.cancelContextFunc()
}
