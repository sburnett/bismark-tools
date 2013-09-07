package commands

import (
	"fmt"
	"github.com/sburnett/bismark-tools/bdmq/datastore"
	"os"
	"text/tabwriter"
)

type countries struct{}

func NewCountries() BdmCommand {
	return new(countries)
}

func (countries) Name() string {
	return "countries"
}

func (countries) Description() string {
	return "Summarize countries"
}

func (countries) Run(args []string) error {
	db, err := datastore.NewPostgresDatastore()
	if err != nil {
		return fmt.Errorf("Error connecting to Postgres database: %s", err)
	}
	defer db.Close()

	writer := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	defer writer.Flush()
	fprintWithTabs(writer, "COUNTRY", "TOTAL", "ONLINE")
	for r := range db.SelectCountries() {
		if r.Error != nil {
			return r.Error
		}
		fprintWithTabs(writer, r.Country, r.Count, r.OnlineCount)
	}
	return nil
}
