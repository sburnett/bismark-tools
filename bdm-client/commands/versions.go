package commands

import (
	"fmt"
	"github.com/sburnett/bismark-tools/bdm-client/datastore"
	"os"
	"text/tabwriter"
)

type versions struct{}

func NewVersions() BdmCommand {
	return new(versions)
}

func (versions) Name() string {
	return "versions"
}

func (versions) Description() string {
	return "Summarize the deployment"
}

func percentage(numerator, denominator int) string {
	return fmt.Sprintf("%d%%", int(float64(numerator)/float64(denominator)*100))
}

func (versions) Run(args []string) error {
	db, err := datastore.NewPostgresDatastore()
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "VERSION", "TOTAL", "ONLINE")
    for r := range db.SelectVersions() {
        if r.Error != nil {
            return r.Error
        }
		fprintWithTabs(writer, r.Version, r.Count, r.OnlineCount)
    }
	return nil
}
