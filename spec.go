package main

import "errors"

var (
	ErrRequiredValue = errors.New("no value for required field")
)

type Spec struct {
	ConsumerGroup string `yaml:"consumerGroup"`

	Streams []*StreamSpec
}

type StreamSpec struct {
	Disabled    bool
	Topic       string
	Table       string
	Key         []string
	Mapping     map[string]*MappingSpec
	PersistKeys bool
}

type MappingSpec struct {
	From     string
	First    []string
	Required bool
}

func (m *MappingSpec) ValueFrom(input map[string]interface{}) (result interface{}, err error) {
	switch {
	case len(m.From) != 0:
		result = input[m.From]

	case len(m.First) != 0:
		for _, key := range m.First {
			if value, ok := input[key]; ok {
				result = value
				break
			}
		}
	}

	if m.Required && result == nil {
		err = ErrRequiredValue
	}

	return
}
