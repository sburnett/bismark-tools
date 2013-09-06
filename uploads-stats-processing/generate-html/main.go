package main

import (
	"flag"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"
)

type Plot struct {
	Path       string
	Experiment string
}

func NewPlot(filename string) Plot {
	basename := filepath.Base(filename)
	return Plot{
		Path:       filepath.Join("uploads-plots", basename),
		Experiment: strings.SplitN(strings.TrimSuffix(basename, ".png"), "-", 2)[1],
	}
}

func GetPlots(plotsDir, plotType string) []Plot {
	filenameGlob := fmt.Sprintf("%s-*.png", plotType)
	plotFilenames, err := filepath.Glob(filepath.Join(plotsDir, filenameGlob))
	if err != nil {
		panic(err)
	}
	plots := []Plot{}
	for _, filename := range plotFilenames {
		plots = append(plots, NewPlot(filename))
	}
	return plots
}

func main() {
	htmlTemplate := flag.String("template", "uploads.html", "Template for generating uploads HTML")
	outputHtml := flag.String("output_html", "/home/sburnett/public_html/bismark-status/uploads.html", "Write HTML file")
	flag.Parse()

	plotsDir := filepath.Join(filepath.Dir(*outputHtml), "uploads-plots")

	plots := map[string][]Plot{
		"counts":        GetPlots(plotsDir, "counts"),
		"interarrivals": GetPlots(plotsDir, "interarrival"),
		"sizes":         GetPlots(plotsDir, "sizes"),
		"usage":         GetPlots(plotsDir, "usage"),
		"dailyusage":    GetPlots(plotsDir, "dailyusage"),
	}

	handle, err := os.Create(*outputHtml)
	if err != nil {
		panic(err)
	}
	defer handle.Close()

	t := template.Must(template.ParseFiles(*htmlTemplate))
	if err := t.Execute(handle, plots); err != nil {
		panic(err)
	}
}
