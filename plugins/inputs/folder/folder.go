package folders

import (
	"fmt"
	"io/ioutil"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal/globpath"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"
)

type Folder struct {
	Folders []string `toml:"folders"`
	parser parsers.Parser

    foldernames []string
	filenames []string
    files []string
}

const sampleConfig = `
  ## Folders to parse each interval.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   /var/log/**.log     -> recursively find all .log files in /var/log
  ##   /var/log/*/*.log    -> find all .log files with a parent dir in /var/log
  ##   /var/log/apache.log -> only read the apache log file
  files = ["/var/log/apache/access.log"]

  ## The dataformat to be read from files
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"
`

// SampleConfig returns the default configuration of the Input
func (f *Folder) SampleConfig() string {
	return sampleConfig
}

func (f *Folder) Description() string {
	return "Reload and gather from file[s] on telegraf's interval."
}

func (f *Folder) Gather(acc telegraf.Accumulator) error {
	err := f.refreshFolderPaths()
    err := f.refreshFilePaths()
	if err != nil {
		return err
	}
	for _, k := range f.filenames {
		metrics, err := f.readMetric(k)
		if err != nil {
			return err
		}

		for _, m := range metrics {
			acc.AddFields(m.Name(), m.Fields(), m.Tags(), m.Time())
		}
	}
	return nil
}

func (f *Folder) SetParser(p parsers.Parser) {
	f.parser = p
}

func (f *Folder) refreshFilePaths() error {
    var all_files []string
    for _, folder_name := range f.foldernames {
        files, err := ioutil.ReadDir(folder_name)
        if err != nil {
            return fmt.Errorf("cannot read folder %v: %v", folder_name, err)
        }
        for _, file_info := range files {
            file_path := fmt.Sprintf("%s%s",folder_name, file_info.Name())
            g, err := globpath.Compile(file_path)
            files := g.Match()
            if err != nil {
                return fmt.Errorf("could not compile glob %v: %v", file_path, err)
            }
            all_files = append(all_files, files...)
        }
    }
    f.filenames = all_files
}


func (f *Folder) refreshFolderPaths() error {
	var all_folders []string
	for _, folder := range f.Folders {
		g, err := globpath.Compile(folder)
		if err != nil {
			return fmt.Errorf("could not compile glob %v: %v", folder, err)
		}
		folders := g.Match()
		if len(folders) <= 0 {
			return fmt.Errorf("could not find folder: %v", folder)
		}
		all_folders = append(all_folders, folders...)
	}

	f.foldernames = all_folders
	return nil
}

func (f *Folder) readMetric(filename string) ([]telegraf.Metric, error) {
	fileContents, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("E! Error file: %v could not be read, %s", filename, err)
	}
	return f.parser.Parse(fileContents)

}

func init() {
	inputs.Add("folder", func() telegraf.Input {
		return &Folder{}
	})
}
