package common

import (
	"github.com/lyft/flytestdlib/contextutils"
)

func GetValidLabelNames() []contextutils.Key {
	return []contextutils.Key{
		contextutils.AppNameKey,
		contextutils.NamespaceKey,
	}
}

func DuplicateMap(o map[string]string) (r map[string]string) {
	if o == nil {
		return map[string]string{}
	}
	r = make(map[string]string, len(o))
	for k, v := range o {
		r[k] = v
	}
	return
}

func CopyMap(to map[string]string, from map[string]string) map[string]string {
	if len(to) == 0 && len(from) == 0 {
		return to
	}
	if len(from) == 0 {
		return to
	}
	if len(to) == 0 {
		to = make(map[string]string, len(from))
	}
	for k, v := range from {
		to[k] = v
	}
	return to
}
