package statsfiledatas

import(
	errors   "github.com/pkg/errors"
	"bufio"
	"os"
	"strings"
	"strconv"
)

type StatsFilesDatas struct {
	Vars  map[string][]float64
	Stats map[string][]float64
}

func NewStatsFilesDatas() *StatsFilesDatas {
	return &StatsFilesDatas{
		Vars: make(map[string][]float64),
		Stats: make(map[string][]float64),
	}
}

func (fDatas *StatsFilesDatas) InitStats(statsNames []string) error {
	if len(fDatas.Stats) != 0 {
		return errors.New("the map object must be empty for being initialized")
	}

	accLen := 1
	for _, vars := range fDatas.Vars {
		accLen = accLen * len(vars)
	}

	nbStats := len(statsNames)
	for i := 0; i < nbStats; i++ {
		fDatas.Stats[statsNames[i]] = make([]float64, accLen)
	}

	return nil
}

func (fDatas *StatsFilesDatas) Serialize(file string) error {
	f, err := os.Create(file)
	if err != nil {
		return errors.Wrapf(err, "Fail to create file %s", file)
	}

	serializeSection(f, fDatas.Vars)
	serializeSection(f, fDatas.Stats)

	f.Close()

	return nil

}

func serializeSection(f *os.File, tab map[string][]float64) {
	f.WriteString(strconv.FormatInt(int64(len(tab)), 10) + "\n")
	for name, vals := range tab {
		f.WriteString(name)
		for _, val := range vals {
			f.WriteString(" " + strconv.FormatFloat(val, 'f', -1, 64))
		}
		f.WriteString("\n")
	}
}

func (fDatas *StatsFilesDatas) Deserialize(file string) error {
	fd, err := os.Open(file)
	if err != nil {
		return errors.Wrapf(err, "Fail to open file %n", file)
	}

	scanner := bufio.NewScanner(fd)

	if err := deserializeSection(scanner, fDatas.Vars); err != nil {
		return err
	}

	if err := deserializeSection(scanner, fDatas.Stats); err != nil {
		return err
	}

	return nil
}

func deserializeSection(scanner *bufio.Scanner, tab map[string][]float64) error {
	if scanner.Scan() == false {
		return getScanError(scanner)
	}

	vTmp, err := strconv.ParseInt(scanner.Text(), 10, 0)
	v := int(vTmp)
	if err != nil {
		return errors.Wrapf(err, "Expecting int for val: %s", scanner.Text())
	}

	for v != 0 && scanner.Scan() {
		toks := strings.Split(scanner.Text(), " ")

		toksLen := len(toks)
		if toksLen < 2 {
			return errors.New("Bad line")
		}

		key := toks[0]
		elems := make([]float64, toksLen - 1)
		for i := 1; i < toksLen; i++ {
			elem, err := strconv.ParseFloat(toks[i], 64)
			if err != nil {
				return errors.Wrapf(err, "Expecting float64 for val: %s", toks[i])
			}
			elems[i - 1] = elem
		}
		tab[key] = elems
		v--
	}

	if v != 0 {
		return getScanError(scanner)
	}

	return nil
}

func getScanError(scanner *bufio.Scanner) error {
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "Error reading file")
	}
	return errors.New("Unexpected end of file")
}
