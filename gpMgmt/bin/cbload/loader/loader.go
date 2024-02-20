package loader

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/cloudberrydb/cbload/log"
	"github.com/cloudberrydb/cbload/option"
	"github.com/cloudberrydb/cbload/worker"
	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

type Loader struct {
	option  *option.Option
	conns   []*pgconn.PgConn
	workMgr *worker.WorkManager
}

func NewLoader() *Loader {
	l := &Loader{}

	l.option = option.NewOption()

	return l
}

func (l *Loader) makeConnection() (*pgconn.PgConn, error) {
	url := fmt.Sprintf("application_name=cbload user=%v password='%v' host=%v port=%v dbname=%v",
		l.option.User, l.option.Password, l.option.Host, l.option.Port, l.option.Database)

	config, err := pgconn.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	conn, err := pgconn.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (l *Loader) listFiles(prefix string, patterns []string) ([]string, []string, error) {
	files := make(map[string]struct{})

	for _, p := range patterns {
		matches, err := filepath.Glob(p)
		if err != nil {
			return nil, nil, err
		}

		for _, f := range matches {
			files[f] = struct{}{}
		}
	}

	localFiles := make([]string, 0)
	destFiles := make([]string, 0)
	for k, _ := range files {
		localFiles = append(localFiles, k)
		destFiles = append(destFiles, filepath.Join(prefix, k))
	}

	return localFiles, destFiles, nil
}

func (l *Loader) populateFiles(prefix string, files []string) ([]string, []string, error) {
	isDirMode, err := l.option.IsDirectoryMode()
	if err != nil {
		return nil, nil, err
	}

	if !isDirMode {
		return l.listFiles(prefix, files)
	}

	localFiles := make([]string, 0)
	destFiles := make([]string, 0)
	err = filepath.Walk(files[0], func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if info.Size() == 0 {
			log.Logger.Infof("Ignore empty file: \"%v\"", path)
			return nil
		}

		localFiles = append(localFiles, path)

		relPath, _ := filepath.Rel(files[0], path)
		destFiles = append(destFiles, filepath.Join(prefix, relPath))
		return nil
	})

	if err != nil {
		return nil, nil, err
	}
	return localFiles, destFiles, nil
}

func (l *Loader) dispatchFiles(localFiles, destFiles []string) ([][]string, [][]string) {
	localTaskFiles := make([][]string, l.option.NumTasks)
	destTaskFiles := make([][]string, l.option.NumTasks)
	for i := 0; i < l.option.NumTasks; i++ {
		localTaskFiles[i] = make([]string, 0)
		destTaskFiles[i] = make([]string, 0)
	}

	for i, file := range localFiles {
		j := i % l.option.NumTasks
		localTaskFiles[j] = append(localTaskFiles[j], file)
		destTaskFiles[j] = append(destTaskFiles[j], destFiles[i])
	}

	return localTaskFiles, destTaskFiles
}

func (l *Loader) start() (chan struct{}, chan struct{}, error) {
	// step 0 retrieve files
	lTotalFiles, rTotalFiles, err := l.populateFiles(l.option.DestPath, l.option.InputFile)
	if err != nil {
		return nil, nil, err
	}

	if len(lTotalFiles) == 0 {
		return nil, nil, nil
	}

	// step 1 dispatch files
	if len(lTotalFiles) < l.option.NumTasks {
		l.option.NumTasks = len(lTotalFiles)
	}
	localTaskFiles, destTaskFiles := l.dispatchFiles(lTotalFiles, rTotalFiles)

	// step 2 making connection
	l.conns = make([]*pgconn.PgConn, l.option.NumTasks)
	log.Logger.Debug("Making database connection...")
	for i := 0; i < len(l.conns); i++ {
		conn, err := l.makeConnection()
		if err != nil {
			return nil, nil, err
		}

		l.conns[i] = conn
	}
	log.Logger.Debug("Successfully connected to database")

	// step 3 create worker
	log.Logger.Debug("Creating worker instance...")
	l.workMgr = worker.NewWorkManager(l.option.Table, l.option.Tag, l.conns, localTaskFiles, destTaskFiles, l.option.StopOnError)
	doneCh, quickDieCh := l.workMgr.Start()
	log.Logger.Debug("Worker instances created successfully")

	return doneCh, quickDieCh, nil
}

func (l *Loader) stop() {
	// stop worker
	log.Logger.Debug("Stopping worker instance...")
	if l.workMgr != nil {
		l.workMgr.Stop()
	}
	log.Logger.Debug("Worker instances have stopped")

	// closing connection
	log.Logger.Debug("Closing database connection...")
	for _, conn := range l.conns {
		if conn != nil {
			conn.Close(context.Background())
		}
	}
	log.Logger.Debug("Database connections have closed")
}

func (l *Loader) Initialize(cmd *cobra.Command) {
	flagSet := cmd.Flags()

	flagSet.StringVar(&l.option.Host, option.HOST, "", "Host to connect to (default localhost)")
	flagSet.IntVar(&l.option.Port, option.PORT, 0, "Port to connect to (default 5432)")
	flagSet.StringVar(&l.option.User, option.USER, "", "User to connect as (default gpadmin)")
	flagSet.StringVar(&l.option.Database, option.DATABASE, "", "Database to connect to (default gpadmin)")
	flagSet.BoolVar(&l.option.ForcePasswordAuth, option.FORCE_PASSWORD_AUTH, false, "Force a password prompt (default false)")
	flagSet.StringVar(&l.option.Table, option.TABLE, "", "Table to load to")
	flagSet.StringVar(&l.option.Tag, option.TAG, "", "File tag")
	flagSet.StringVar(&l.option.DestPath, option.DEST_PATH, "", "Path relative to the table root directory (default: root directory of the table)")
	flagSet.StringSliceVar(&l.option.InputFile, option.INPUT_FILE, []string{}, "Input files or directory")
	flagSet.IntVar(&l.option.NumTasks, option.TASKS, 1, "The maximum number of files that concurrently loads")
	flagSet.StringVar(&l.option.LogFile, option.LOGFILE, "", "Log output to logfile (default none)")
	flagSet.BoolVar(&l.option.StopOnError, option.STOP_ON_ERROR, false, "Stop loading files when an error occurs (default false)")
	flagSet.BoolVar(&l.option.Verbose, option.VERBOSE, false, "Indicates that the tool should generate verbose output (default false)")
	flagSet.Bool("help", false, "Print help info and exit")
	flagSet.Bool("version", false, "Print version info and exit")

	cmd.MarkFlagRequired(option.TABLE)
	cmd.MarkFlagRequired(option.INPUT_FILE)
}

func (l *Loader) GetVersion() string {
	return "1.0.0"
}

func (l *Loader) Run(cmd *cobra.Command, args []string) {
	// initialize log
	log.Initialize(l.option.LogFile, l.option.Verbose)
	defer log.ShutDown()

	// parse options
	err := l.option.Parse()
	if err != nil {
		log.Logger.Fatalf("%v", err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// start loader
	doneCh, quickDieCh, err := l.start()
	if err != nil {
		log.Logger.Fatalf("Failed to start loader: %v", err)
	}

	if doneCh != nil {
	loop:
		for {
			select {
			case <-interrupt:
				err = errors.New("Caught interrupt signal, exitting...")
				log.Logger.Errorf("%v", err)
				break loop
			case <-quickDieCh:
				err = errors.New("Caught internal error, exitting...")
				log.Logger.Errorf("%v", err)
				break loop
			case <-doneCh:
				log.Logger.Infof("successfully loaded %v files, failed %v files",
					l.workMgr.GetNumSucceedFiles(), l.workMgr.GetNumFailedFiles())
				break loop
			case <-time.After(25 * time.Millisecond):
				log.Logger.Debug("Event loop")
			}
		}
	}

	l.stop()

	if err != nil {
		os.Exit(1)
	}
}
