package commands

type BdmCommand interface {
	Name() string
	Description() string
	Run(args []string) error
}
