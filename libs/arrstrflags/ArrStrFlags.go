package arrstrflags

import(
	"bytes"
)

type ArrStrFlags []string

func (i *ArrStrFlags) String() string {
	var buffer bytes.Buffer

	first := true
	for _, s := range *i {
		if first == false {
			buffer.WriteString(",")
		} else {
			first = false
		}
		buffer.WriteString(s)
	}

	return buffer.String()
}

func (i *ArrStrFlags) Set(value string) error {
    *i = append(*i, value)
    return nil
}
