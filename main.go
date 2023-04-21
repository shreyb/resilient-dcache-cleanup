package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	//"github.com/knadh/koanf"
	htcondor "github.com/retzkek/htcondor-go"
	classad "github.com/retzkek/htcondor-go/classad"
	"github.com/rs/zerolog/log"
)

// TODO Eventually should come from config
var groups []string = []string{"gm2", "sbnd"}
var groupDcacheAreaOverride map[string]string = map[string]string{
	"gm2": "/pnfs/GM2/resilient/jobsub_stage",
}

// TODO This should be configurable
const scheddConstraint string = "InDowntime==false && IsJobsubLite==true"

// Set groups to dCache area override

func main() {
	// TODO Eventually add a timeout here
	ctx := context.Background()
	schedds, err := getPoolSchedds(ctx)
	if err != nil {
		log.Fatal().Msg("Could not get schedds.  Exiting")
	}

	activeFiles := make(map[string]fileSet, 0)
	// var groupsWg sync.WaitGroup
	// TODO Make the groups loop concurrent
	for _, group := range groups {

		// Run htgettoken
		if err := getToken(ctx, group); err != nil {
			log.Error().Err(err).Msgf("Could not get bearer token for group %s", group)
		}

		activeFiles[group] = NewFileSet()

		fileChan := make(chan string)
		var filesWgByGroup sync.WaitGroup

		// Listener to add files to our group fileSet
		go func(fileChan chan string) {
			for f := range fileChan {
				activeFiles[group].AddFile(f)
			}
		}(fileChan)

		for _, schedd := range schedds {
			filesWgByGroup.Add(1)
			go func(group, schedd string) {
				defer filesWgByGroup.Done()
				files, err := getAllActiveFilesByGroupAndSchedd(ctx, group, schedd)
				if err != nil {
					log.Error().Err(err).Msgf("Could not get all active files for group %s and schedd %s", group, schedd)
				}
				for _, f := range files {
					fileChan <- f
				}
			}(group, schedd)
		}
		filesWgByGroup.Wait()
		close(fileChan)
	}
	fmt.Println(activeFiles)

}

func getPoolSchedds(ctx context.Context) ([]string, error) {
	// Get all schedds
	getScheddsCommand := htcondor.NewCommand("/usr/bin/condor_status").
		WithArg("-schedd").
		WithConstraint(scheddConstraint).
		WithAttribute("Name")

	scheddAds, err := getScheddsCommand.RunWithContext(ctx)
	if err != nil {
		log.Error().Msg("Could not get schedds to find actively-used files on.  Exiting")
		return nil, err
	}

	schedds := make([]string, 0, len(scheddAds))
	for _, scheddAd := range scheddAds {
		schedd := scheddAd["Name"].String()
		schedds = append(schedds, schedd)
	}
	return schedds, nil
}

func getToken(ctx context.Context, group string) error {
	// Run htgettoken
	htgettokenCmd := exec.CommandContext(ctx, "htgettoken", "-a", "htvaultprod.fnal.gov", "-i", group)
	fmt.Println(htgettokenCmd.String())
	htgettokenCmd.Stdout = os.Stdout
	htgettokenCmd.Stderr = os.Stderr
	// log.Debug().Msg(htgettokenCmd.Args)
	// TODO Eventually this needs to be async - using CombinedOutput.  Only possible after we do the special kerberos principals
	if err := htgettokenCmd.Start(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			log.Error().Msg("timeout error")
			return ctx.Err()
		}
		log.Error().Err(err).Msg("Error starting htgettoken command")
		return err
	}

	if err := htgettokenCmd.Wait(); err != nil {
		log.Error().Err(err).Msg("Error running htgettoken command")
	}
	return nil
}

// Get All files on resilient dCache

// Adapted from https://www.davidkaya.com/p/sets-in-golang
type fileSet map[string]struct{}

func NewFileSet() fileSet {
	f := make(fileSet, 0)
	return f
}

func (f fileSet) AddFile(filename string) {
	f[filename] = struct{}{}
}

func (f fileSet) RemoveFile(filename string) {
	delete(f, filename)
}

func (f fileSet) String() string {
	fSlice := make([]string, 0, len(f))
	for k, _ := range f {
		fSlice = append(fSlice, k)
	}
	return fmt.Sprintf("fileSet{%s}", strings.Join(fSlice, ", "))
}

// func (f *fileSet) Intersect(f2 *fileSet) *fileSet {}
// func (f *fileSet) SymmetricDifference(f2 *fileSet) *fileSet{}

func isUndefinedAttribute(a classad.Attribute) bool {
	if a.Type == classad.Undefined {
		return true
	}
	if a.Type == classad.String {
		val, ok := a.Value.(string)
		if ok && val == "undefined" {
			return true
		}
	}
	return false
}

func extractActiveFilesFromClassad(c classad.ClassAd) ([]string, error) {
	a := c["PNFS_INPUT_FILES"]
	if isUndefinedAttribute(a) {
		return nil, &undefinedAttributeError{}
	}
	if a.Type != classad.String {
		return nil, errors.New("unsupported classad attribute type.  Supported types are string, undefined")
	}

	val, ok := a.Value.(string)
	if !ok {
		return nil, fmt.Errorf("classad attribute value is of wrong type.  Expected string, got %T", val)

	}
	files := strings.Split(val, ",")
	return files, nil
}

func getAllActiveFilesByGroupAndSchedd(ctx context.Context, group, schedd string) ([]string, error) {
	files := make([]string, 0)
	groupConstraint := fmt.Sprintf("Jobsub_Group==\"%s\"", group)
	activeFilesCmd := htcondor.NewCommand("/usr/bin/condor_q").
		WithName(schedd).
		WithConstraint(groupConstraint).
		WithAttribute("PNFS_INPUT_FILES")

	activeFilesAds, err := activeFilesCmd.RunWithContext(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Could not run condor_q command to get PNFS_INPUT_FILES values")
		return nil, err
	}

	for _, activeFileAd := range activeFilesAds {
		classadFiles, err := extractActiveFilesFromClassad(activeFileAd)
		if err != nil {
			var e *undefinedAttributeError
			if errors.As(err, &e) {
				continue // Got undefined classad attribute.  Ignore it
			}
			log.Error().Err(err).Msg("could not extract active files from job classad")
			return nil, err
		}
		files = append(files, classadFiles...)
	}
	return files, nil
}

type undefinedAttributeError struct{}

func (u *undefinedAttributeError) Error() string {
	return "undefined"
}
