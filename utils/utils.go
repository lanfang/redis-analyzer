package utils

import (
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
	"reflect"
	"strings"
	"unsafe"
)

func String(b []byte) (s string) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pstring.Data = pbytes.Data
	pstring.Len = pbytes.Len
	return
}

func Slice(s string) (b []byte) {
	pbytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pstring := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pbytes.Data = pstring.Data
	pbytes.Len = pstring.Len
	pbytes.Cap = pstring.Len
	return
}

type Formater interface {
	Format() string
}

func NewResultWriter(base_file, extra string) (*ResultWriter, error) {
	var writer *os.File
	var err error
	is_file := false
	for loop := true; loop; loop = false {
		if base_file != "" {
			if extra != "" {
				base_file = fmt.Sprintf("%v_%v", base_file, extra)
			}
			if fwriter, err2 := os.OpenFile(base_file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666); err2 == nil {
				writer = fwriter
			} else {
				err = err2
				break
			}
			is_file = true
		} else {
			writer = os.Stdout
		}
	}
	if err != nil {
		return nil, err
	}
	return &ResultWriter{Output: writer, fwriter: is_file}, nil

}

type ResultWriter struct {
	fwriter        bool
	Output         *os.File
	isHeaderWrited bool
}

func (r *ResultWriter) Write(name string, header []string, rows [][]string, pretty bool) {
	if pretty {
		r.prettyWrite(name, header, rows)
	} else {
		r.normalWrite(name, header, rows)
	}

}
func (r *ResultWriter) prettyWrite(name string, header []string, rows [][]string) {
	table := tablewriter.NewWriter(r.Output)
	table.SetHeader(header)
	for _, row := range rows {
		table.Append(row)
	}
	table.SetCaption(true, name)
	table.SetAutoMergeCells(true)
	table.SetRowLine(true)
	table.Render()
}

func (r *ResultWriter) normalWrite(name string, header []string, rows [][]string) {
	fmt.Fprintln(r.Output, fmt.Sprintf("%v:", name))
	fmt.Fprintln(r.Output, fmt.Sprintf("%v", strings.Join(header, " ")))
	for _, row := range rows {
		fmt.Fprintln(r.Output, fmt.Sprintf("%v", strings.Join(row, " ")))
	}

}

func (r *ResultWriter) FormatWrite(header string, row Formater) {
	if !r.isHeaderWrited && header != "" {
		fmt.Fprintln(r.Output, header)
		r.isHeaderWrited = true
	}
	fmt.Fprintln(r.Output, row.Format())
}

func (r *ResultWriter) Close() {
	if r.fwriter && r.Output != nil {
		r.Output.Close()
	}
}
