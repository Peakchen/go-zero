package format

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/Peakchen/peakchen-go-zero/tools/goctl/pkg/parser/api/parser"
)

// File formats the api file.
func File(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(nil)
	if err := Source(data, buffer); err != nil {
		return err
	}
	return ioutil.WriteFile(filename, buffer.Bytes(), 0666)
}

// Source formats the api source.
func Source(source []byte, w io.Writer) error {
	p := parser.New("", source)
	result := p.Parse()
	if err := p.CheckErrors(); err != nil {
		return err
	}

	result.Format(w)
	return nil
}

func formatForUnitTest(source []byte, w io.Writer) error {
	p := parser.New("", source)
	result := p.Parse()
	if err := p.CheckErrors(); err != nil {
		return err
	}

	result.FormatForUnitTest(w)
	return nil
}
