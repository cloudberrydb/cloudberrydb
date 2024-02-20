package option

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/cloudberrydb/cbload/log"
	"github.com/pkg/errors"
	"golang.org/x/term"
)

const (
	HOST                = "host"
	PORT                = "port"
	USER                = "user"
	DATABASE            = "database"
	FORCE_PASSWORD_AUTH = "force-password-auth"
	TABLE               = "table"
	TAG                 = "tag"
	DEST_PATH           = "dest-path"
	INPUT_FILE          = "input-file"
	TASKS               = "tasks"
	LOGFILE             = "logfile"
	STOP_ON_ERROR       = "stop-on-error"
	VERBOSE             = "verbose"
)

type Option struct {
	Host              string
	Port              int
	User              string
	Database          string
	Password          string
	ForcePasswordAuth bool
	Table             string
	Tag               string
	DestPath          string
	InputFile         []string
	LogFile           string
	NumTasks          int
	StopOnError       bool
	Verbose           bool
}

func NewOption() *Option {
	o := &Option{}

	return o
}

func (o *Option) Parse() error {
	var err error

	o.Host = o.getHostOrDefault(o.Host)
	o.Port = o.getPortOrDefault(o.Port)
	o.User = o.getUserOrDefault(o.User)
	o.Database = o.getDatabaseOrDefault(o.Database)
	o.Password, err = o.getPasswordOrDefault(o.ForcePasswordAuth)
	if err != nil {
		return err
	}

	if o.NumTasks > 256 || o.NumTasks < 1 {
		return errors.Errorf("Invalid value: \"%v\" for --tasks option, the number of tasks must be in the range of 1 to 256", o.NumTasks)
	}

	return nil
}

func (o *Option) getHostOrDefault(host string) string {
	result := host

	if len(result) == 0 {
		result = os.Getenv("PGHOST")
	}

	if len(result) == 0 {
		result = "localhost"
	}

	return result
}

func (o *Option) getPortOrDefault(port int) int {
	result := port

	if result == 0 {
		p, err := strconv.Atoi(os.Getenv("PGPORT"))
		if err == nil {
			result = p
		}
	}

	if result == 0 {
		result = 5432
	}

	return result
}

func (o *Option) getUserOrDefault(user string) string {
	result := user

	if len(result) == 0 {
		result = os.Getenv("PGUSER")
	}

	if len(result) == 0 {
		result = os.Getenv("USER")
		if len(o.User) == 0 {
			result = os.Getenv("LOGNAME")
		}
		if len(o.User) == 0 {
			result = os.Getenv("USERNAME")
		}
	}

	if len(result) == 0 {
		result = "gpadmin"
	}

	return result
}

func (o *Option) getDatabaseOrDefault(database string) string {
	result := database

	if len(result) == 0 {
		result = os.Getenv("PGDATABASE")
	}

	if len(result) == 0 {
		result = o.User
	}

	return result
}

func (o *Option) getPasswordOrDefault(forcePasswordAuth bool) (string, error) {
	result := ""
	var err error

	if forcePasswordAuth {
		fmt.Printf("Password: ")
		bp, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Printf("\n")
		if err != nil {
			return result, err
		}
		result = string(bp)
	}

	if len(result) == 0 {
		result = os.Getenv("PGPASSWORD")
	}

	if len(result) == 0 {
		passFile := os.Getenv("PGPASSFILE")
		if len(passFile) == 0 {
			dir := os.Getenv("HOME")
			if len(dir) == 0 {
				dir = "."
			}
			passFile = dir + "/.pgpass"
		}

		result, err = o.readPGPass(passFile)
		if err != nil {
			return result, err
		}
	}

	return result, nil
}

func (o *Option) readPGPass(file string) (string, error) {
	password := ""

	f, err := os.Open(file)
	if err != nil {
		log.Logger.Debugf("%v", err)
		return password, nil
	}

	defer f.Close()
	sc := bufio.NewScanner(f)

	for sc.Scan() {
		elems := o.splitPGPassLine(sc.Text())
		if len(elems) != 5 {
			return password, errors.Errorf("pgpass file: invalid line \"%v\":there should be 5 fields in a line, seperated by colon", sc.Text())
		}

		if elems[0] != "*" && strings.ToLower(elems[0]) != strings.ToLower(o.Host) {
			continue
		}

		if elems[1] != "*" {
			p, err := strconv.Atoi(elems[1])

			if err != nil {
				return password, errors.Errorf("pgpass file: invalid line \"%v\":port number should be integer", sc.Text())
			}

			if p != o.Port {
				continue
			}
		}

		if elems[2] != "*" && elems[2] != o.Database {
			continue
		}

		if elems[3] != "*" && elems[3] != o.User {
			continue
		}

		password = elems[4]
		break
	}

	return password, nil
}

func (o *Option) splitPGPassLine(line string) []string {
	escape := false
	results := make([]string, 0)
	elem := make([]byte, 0)
	bline := []byte(line)

	for _, c := range bline {
		if !escape && c == '\\' {
			escape = true
		} else if !escape && c == ':' {
			results = append(results, string(elem))
			elem = make([]byte, 0)
		} else {
			elem = append(elem, c)
			escape = false
		}
	}

	if escape {
		elem = append(elem, '\\')
	}

	results = append(results, string(elem))

	return results
}

func (o *Option) IsDirectoryMode() (bool, error) {
	numDirs := 0
	for _, file := range o.InputFile {
		stat, err := os.Stat(file)
		if err != nil {
			return false, err
		}

		if stat.IsDir() {
			numDirs++
		}
	}

	if len(o.InputFile) == 1 {
		if numDirs == 1 {
			return true, nil
		}

		return false, nil
	}

	if numDirs == len(o.InputFile) {
		return false, errors.Errorf("Only one directory can be specified")
	}

	if numDirs > 0 {
		return false, errors.Errorf("File and directory cannot be specified at the same time")
	}

	return false, nil
}
