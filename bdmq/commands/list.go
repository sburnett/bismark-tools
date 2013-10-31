package commands

type list struct{}

func NewList() BdmCommand {
	return new(list)
}

func (list) Name() string {
	return "list"
}

func (list) Description() string {
	return "List all devices"
}

func (list) Run(args []string) error {
	devices := NewDevices()
	return devices.Run([]string{})
}
