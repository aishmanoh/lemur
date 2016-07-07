package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/dustin/go-humanize"

	"github.intel.com/hpdd/logging/applog"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/hsm"
)

func init() {
	hsmStateFlags := strings.Join(hsm.GetStateFlagNames(), ",")

	hsmCommand := cli.Command{
		Name:  "hsm",
		Usage: "HSM-related data movement actions",
		Subcommands: []cli.Command{
			{
				Name:      "archive",
				Usage:     "Initiate HSM archive of specified paths",
				ArgsUsage: "[path [path...]]",
				Flags: []cli.Flag{
					cli.IntFlag{
						Name:  "id, i",
						Usage: "Numeric ID of archive backend",
					},
					cli.BoolFlag{
						Name:  "null, 0",
						Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
					},
				},
				Action: hsmRequestAction(hsm.RequestArchive),
			},
			{
				Name:      "release",
				Usage:     "Release local data of HSM-archived paths",
				ArgsUsage: "[path [path...]]",
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "null, 0",
						Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
					},
				},
				Action: hsmRequestAction(hsm.RequestRelease),
			},
			{
				Name:      "restore",
				Usage:     "Explicitly restore local data of HSM-archived paths",
				ArgsUsage: "[path [path...]]",
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "null, 0",
						Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
					},
				},
				Action: hsmRequestAction(hsm.RequestRestore),
			},
			{
				Name:      "remove",
				Usage:     "Remove HSM-archived data of specified paths (local data is not removed)",
				ArgsUsage: "[path [path...]]",
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "null, 0",
						Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
					},
				},
				Action: hsmRequestAction(hsm.RequestRemove),
			},
			{
				Name:      "cancel",
				Usage:     "Cancel HSM operations being performed on specified paths",
				ArgsUsage: "[path [path...]]",
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "null, 0",
						Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
					},
				},
				Action: hsmRequestAction(hsm.RequestCancel),
			},
			{
				Name:      "set",
				Usage:     "Set HSM flags or archive ID for specified paths",
				ArgsUsage: "[path [path...]]",
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "null, 0",
						Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
					},
					cli.IntFlag{
						Name:  "id, i",
						Usage: "Numeric ID of archive backend",
					},
					cli.StringSliceFlag{
						Name:  "flag, f",
						Usage: fmt.Sprintf("HSM flag to set (%s)", hsmStateFlags),
						Value: &cli.StringSlice{},
					},
					cli.StringSliceFlag{
						Name:  "clear, F",
						Usage: fmt.Sprintf("HSM flag to clear (%s)", hsmStateFlags),
						Value: &cli.StringSlice{},
					},
				},
				Action: hsmSetAction,
			},
			{
				Name:      "status",
				Usage:     "Display HSM status for specified paths",
				ArgsUsage: "[path [path...]]",
				Flags: []cli.Flag{
					cli.BoolFlag{
						Name:  "action, a",
						Usage: "Include current HSM action",
					},
					cli.BoolFlag{
						Name:  "hide-path, H",
						Usage: "Hide pathname in output",
					},
					cli.BoolFlag{
						Name:  "long, l",
						Usage: "Show long-form states",
					},
					cli.BoolFlag{
						Name:  "progress, p",
						Usage: "Show copy progress for archive/restore actions",
					},
					cli.BoolFlag{
						Name:  "null, 0",
						Usage: "Null-separated paths are read from stdin (e.g. piped from find -print0)",
					},
				},
				Action: hsmStatusAction,
			},
		},
	}
	commands = append(commands, hsmCommand)
}

func getFilePaths(c *cli.Context) ([]string, error) {
	var paths []string

	if c.Bool("null") {
		reader := bufio.NewReader(os.Stdin)
		path, err := reader.ReadBytes('\000')
		for err == nil {
			paths = append(paths, string(path[:len(path)-1]))
			path, err = reader.ReadBytes('\000')
		}
		if err != io.EOF {
			return nil, err
		}
	} else {
		paths = c.Args()
	}

	return paths, nil
}

func getPathStatus(c *cli.Context, filePath string) (string, error) {
	var buf bytes.Buffer

	s, err := hsm.GetFileStatus(filePath)
	if err != nil {
		return "", err
	}

	if !c.Bool("hide-path") {
		fmt.Fprintf(&buf, "%s ", filePath)
	}
	fmt.Fprintf(&buf, hsm.FileStatusString(s, !c.Bool("long")))

	if s.Exists() && c.Bool("action") {
		a, err := hsm.GetFileAction(filePath)
		if err != nil {
			return "", err
		}
		if a.IsNone() {
			fmt.Fprintf(&buf, " -")
		} else {
			fmt.Fprintf(&buf, " [%s:%s]", a.Action(), a.State())
			if c.Bool("progress") && (a.IsArchive() || a.IsRestore()) {
				st, err := os.Stat(filePath)
				if err != nil {
					applog.Fail(err)
				}
				fmt.Fprintf(&buf, "(%s/%s)",
					humanize.IBytes(a.BytesCopied),
					humanize.IBytes(uint64(st.Size())))
			}
		}
	} else {
		fmt.Fprintf(&buf, " -")
	}

	// TODO: Display xattrs, once we've standardized them?

	return buf.String(), nil
}

func hsmSetAction(c *cli.Context) {
	logContext(c)

	paths, err := getFilePaths(c)
	if err != nil {
		applog.Fail(err)
	}

	if len(paths) < 1 {
		applog.Fail(fmt.Errorf("HSM set request must be made with at least 1 path"))
	}

	setFlags, err := hsm.GetStatusMask(c.StringSlice("flag"))
	if err != nil {
		applog.Fail(err)
	}
	clearFlags, err := hsm.GetStatusMask(c.StringSlice("clear"))
	if err != nil {
		applog.Fail(err)
	}
	archiveID := uint32(c.Int("id"))

	if setFlags == 0 && clearFlags == 0 && archiveID == 0 {
		applog.Fail(fmt.Errorf("HSM set request made with no flags to set or clear, and no new archive ID supplied"))
	}

	// TODO: Parallelize this?
	for _, path := range paths {
		if err := hsm.SetFileStatus(path, setFlags, clearFlags, archiveID); err != nil {
			applog.Fail(err)
		}
	}
}

func hsmStatusAction(c *cli.Context) {
	logContext(c)

	paths, err := getFilePaths(c)
	if err != nil {
		applog.Fail(err)
	}

	if len(paths) < 1 {
		applog.Fail(fmt.Errorf("HSM status request must be made with at least 1 path"))
	}

	for _, path := range paths {
		status, err := getPathStatus(c, path)
		if err != nil {
			applog.Fail("%s: %v", path, err)
		}
		fmt.Println(status)
	}
}

type hsmRequestFn func(fs.RootDir, uint, []*lustre.Fid) error

func hsmRequestAction(requestFn func(fs.RootDir, uint, []*lustre.Fid) error) func(*cli.Context) {
	return func(c *cli.Context) {
		logContext(c)

		paths, err := getFilePaths(c)
		if err != nil {
			applog.Fail(err)
		}
		err = submitHsmRequest(c.Command.Name, hsmRequestFn(requestFn), uint(c.Int("id")), paths...)
		if err != nil {
			applog.Fail(err)
		}
	}
}

func submitHsmRequest(actionName string, requestFn hsmRequestFn, archiveID uint, paths ...string) error {
	var fids []*lustre.Fid

	if len(paths) < 1 {
		return fmt.Errorf("HSM %s request must be made with at least 1 path", actionName)
	}

	fsRoot, err := fs.MountRoot(paths[0])
	if err != nil {
		return fmt.Errorf("Error getting fs root from %s: %s", paths[0], err)
	}

	// TODO: Occurs to me that it might be better to break up a large
	// batch into multiple batches, each serviced by its own goroutine.
	for _, path := range paths {
		absPath, err := filepath.Abs(path)
		if !strings.HasPrefix(absPath, fsRoot.Path()) {
			return fmt.Errorf("All files in HSM request must be in the same filesystem (%s is not in %s)",
				path, fsRoot)
		}

		fid, err := fs.LookupFid(path)
		if err != nil {
			return fmt.Errorf("Cannot resolve Fid for %s: %s", path, err)
		}
		fids = append(fids, fid)
	}

	if requestFn != nil {
		err = requestFn(fsRoot, archiveID, fids)
	} else {
		err = fmt.Errorf("Unhandled HSM action: %s", actionName)
	}

	return err
}