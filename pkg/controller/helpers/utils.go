package helpers

func CopyMap(o map[string]string) (r map[string]string) {
	if o == nil {
		return map[string]string{}
	}
	r = make(map[string]string, len(o))
	for k, v := range o {
		r[k] = v
	}
	return
}
