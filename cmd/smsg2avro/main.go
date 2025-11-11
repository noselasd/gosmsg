// Command smsg2avro converts SMSG YAML schema files to Avro schema format.
//
// Usage:
//
//	smsg2avro [flags] <schema.yaml>
//
// The tool reads an SMSG schema from a YAML file and outputs the equivalent
// Avro schema as JSON to stdout.
//
// Flags:
//
//	-namespace string
//	    Avro namespace for the schema (optional)
//	-no-metadata
//	    Exclude UTEL:metadata from the output (default: false)
//
// Examples:
//
//	# Convert a schema to Avro
//	smsg2avro schema.yaml
//
//	# Convert with namespace
//	smsg2avro -namespace com.example schema.yaml
//
//	# Convert without metadata
//	smsg2avro -no-metadata schema.yaml
//
//	# Save to file
//	smsg2avro schema.yaml > output.avsc
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/noselasd/gosmsg"
)

var (
	namespace   = flag.String("namespace", "", "Avro namespace for the schema")
	noMetadata  = flag.Bool("no-metadata", false, "Exclude UTEL:metadata from output")
	showHelp    = flag.Bool("help", false, "Show help message")
	showVersion = flag.Bool("version", false, "Show version information")
)

const version = "1.0.0"

func main() {
	flag.Usage = usage
	flag.Parse()

	if *showVersion {
		fmt.Fprintf(os.Stderr, "smsg2avro version %s\n", version)
		os.Exit(0)
	}

	if *showHelp {
		usage()
		os.Exit(0)
	}

	// Check for schema file argument
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "Error: exactly one schema file argument required\n\n")
		usage()
		os.Exit(1)
	}

	schemaFile := flag.Arg(0)

	// Load the SMSG schema
	schema, err := gosmsg.LoadSchema(schemaFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading schema from %s: %v\n", schemaFile, err)
		os.Exit(1)
	}

	// Convert to Avro schema
	addMetadata := !*noMetadata
	avroJSON, err := gosmsg.SchemaToAvroJSON(schema, *namespace, addMetadata)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error converting schema to Avro: %v\n", err)
		os.Exit(1)
	}

	// Output to stdout
	fmt.Println(avroJSON)
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: smsg2avro [flags] <schema.yaml>\n\n")
	fmt.Fprintf(os.Stderr, "Convert SMSG YAML schema files to Avro schema format.\n\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nExamples:\n")
	fmt.Fprintf(os.Stderr, "  smsg2avro schema.yaml\n")
	fmt.Fprintf(os.Stderr, "  smsg2avro -namespace com.example schema.yaml\n")
	fmt.Fprintf(os.Stderr, "  smsg2avro -no-metadata schema.yaml\n")
	fmt.Fprintf(os.Stderr, "  smsg2avro schema.yaml > output.avsc\n")
}
