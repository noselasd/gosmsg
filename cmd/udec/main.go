// Command udec reads a stream of SMSG records and pretty prints them.
//
// Usage:
//
//	udec [flags] [file]
//
// The tool reads SMSG messages from a file or stdin and pretty prints them
// in one of two modes:
//
//  1. Raw mode (default): Shows tags and values without schema interpretation
//  2. Schema mode (-schema): Uses a schema to show field names and typed values
//
// Both modes support verbose output (-v) which includes additional details
// like tag lengths and numeric tag values.
//
// Flags:
//
//	-schema string
//	    YAML schema file for interpreting messages
//	-v, -verbose
//	    Enable verbose output (show tag lengths and numeric tags)
//
// Examples:
//
//	# Raw mode from file
//	udec messages.smsg
//
//	# Raw mode from stdin
//	cat messages.smsg | udec
//
//	# Schema mode
//	udec -schema schema.yaml messages.smsg
//
//	# Verbose schema mode
//	udec -v -schema schema.yaml messages.smsg
//
//	# Verbose raw mode
//	udec -v messages.smsg
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/noselasd/gosmsg"
)

var (
	schemaFile = flag.String("schema", "", "YAML schema file for interpreting messages")
	verbose    = flag.Bool("v", false, "Enable verbose output (show tag lengths and numeric tags)")
	showHelp   = flag.Bool("help", false, "Show help message")
)

func main() {
	flag.BoolVar(verbose, "verbose", false, "Enable verbose output (alias for -v)")
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		usage()
		os.Exit(0)
	}

	// Determine input source
	var input io.Reader
	var inputName string
	if flag.NArg() == 0 {
		input = os.Stdin
		inputName = "stdin"
	} else if flag.NArg() == 1 {
		file, err := os.Open(flag.Arg(0))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
		input = file
		inputName = flag.Arg(0)
	} else {
		fmt.Fprintf(os.Stderr, "Error: too many arguments\n\n")
		usage()
		os.Exit(1)
	}

	// Load schema if provided
	var schema *gosmsg.Schema
	var decoder *gosmsg.SchemaDecoder
	if *schemaFile != "" {
		var err error
		schema, err = gosmsg.LoadSchema(*schemaFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading schema from %s: %v\n", *schemaFile, err)
			os.Exit(1)
		}

		decoder, err = gosmsg.NewSchemaDecoder([]gosmsg.Schema{*schema})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating decoder: %v\n", err)
			os.Exit(1)
		}
	}

	// Read and process messages
	reader := gosmsg.NewRawSMsgReader(input)
	msgCount := 0
	errCount := 0

	for {
		msg, err := reader.ReadRawSMsg()
		if err == gosmsg.EOS {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading message from %s: %v\n", inputName, err)
			errCount++
			continue
		}

		if msgCount > 0 {
			fmt.Println() // Blank line between messages
		}
		msgCount++

		if schema != nil && decoder != nil {
			// Schema mode
			printWithSchema(msg, decoder, schema)
		} else {
			// Raw mode
			printRaw(msg)
		}
	}

	if msgCount == 0 {
		fmt.Fprintf(os.Stderr, "No messages read from %s\n", inputName)
		os.Exit(1)
	}

	if errCount > 0 {
		fmt.Fprintf(os.Stderr, "\n%d errors encountered while reading %d messages\n", errCount, msgCount)
		os.Exit(1)
	}
}

// printRaw prints a message in raw mode without schema interpretation
func printRaw(msg gosmsg.RawSMsg) {
	it := msg.Tags()
	recordTag, err := it.NextTag()
	if err != nil {
		fmt.Printf("Error reading record tag: %v\n", err)
		return
	}

	// Print record header
	if recordTag.Constructor {
		if recordTag.VarLen {
			fmt.Printf("Record: 0x%04X (constructor, varlen)\n", recordTag.Tag)
		} else {
			fmt.Printf("Record: 0x%04X (constructor, length: %d)\n", recordTag.Tag, len(recordTag.Data))
		}
	} else {
		fmt.Printf("Record: 0x%04X (length: %d)\n", recordTag.Tag, len(recordTag.Data))
	}

	subIt := recordTag.SubTags()
	for {
		tag, err := subIt.NextTag()
		if err == gosmsg.EOS {
			break
		}
		if err != nil {
			fmt.Printf("  Error reading tag: %v\n", err)
			break
		}

		if *verbose {
			fmt.Printf("  0x%04X (length: %3d):  %s\n", tag.Tag, len(tag.Data), tag.Data)
		} else {
			fmt.Printf("  0x%04X:  %s\n", tag.Tag, tag.Data)
		}
		if tag.Tag == 0 {
			break // Terminator
		}

	}
}

// printWithSchema prints a message using schema interpretation
func printWithSchema(msg gosmsg.RawSMsg, decoder *gosmsg.SchemaDecoder, schema *gosmsg.Schema) {
	decoded, err := decoder.Decode(msg)
	if err != nil {
		fmt.Printf("Error decoding message: %v\n", err)
		// Try to print what we got
		if decoded != nil {
			fmt.Printf("Partial decode: %s\n", decoded.RecordType)
		}
		return
	}

	// Print record header
	if *verbose {
		fmt.Printf("Record: %s (tag: 0x%04X)\n", decoded.RecordType, decoded.RecordTag)
	} else {
		fmt.Printf("Record: %s (0x%04X)\n", decoded.RecordType, decoded.RecordTag)
	}

	// Collect field info for alignment
	type fieldDisplay struct {
		name     string
		tag      uint16
		value    any
		typeName string
	}
	var displays []fieldDisplay

	for _, field := range schema.Fields {
		value, exists := decoded.Fields[field.Name]
		if !exists {
			continue
		}

		tag := uint16(0)
		if tagVal, ok := field.Metadata["smsg_tag"].(int); ok {
			tag = uint16(tagVal)
		}

		typeName := field.Type.String()

		// Calculate length (approximate from original data)

		displays = append(displays, fieldDisplay{
			name:     field.Name,
			tag:      tag,
			value:    value,
			typeName: typeName,
		})
	}

	// Calculate column widths
	maxNameLen := 0
	maxTypeLen := 0
	for _, d := range displays {
		if len(d.name) > maxNameLen {
			maxNameLen = len(d.name)
		}
		if len(d.typeName) > maxTypeLen {
			maxTypeLen = len(d.typeName)
		}
	}

	// Print fields with alignment
	for _, d := range displays {
		if *verbose {
			fmt.Printf("  %-*s (tag: 0x%04X,%8s):  %v\n",
				maxNameLen, d.name, d.tag, d.typeName, d.value)
		} else {
			fmt.Printf("  %-*s:  %v\n",
				maxNameLen, d.name, d.value)
		}
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: udec [flags] [file]\n\n")
	fmt.Fprintf(os.Stderr, "Read and pretty print SMSG messages from file or stdin.\n\n")
	fmt.Fprintf(os.Stderr, "Modes:\n")
	fmt.Fprintf(os.Stderr, "  Raw mode (default):    Shows tags and values without schema\n")
	fmt.Fprintf(os.Stderr, "  Schema mode (-schema): Uses schema to show field names and types\n\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nExamples:\n")
	fmt.Fprintf(os.Stderr, "  udec messages.smsg\n")
	fmt.Fprintf(os.Stderr, "  cat messages.smsg | udec\n")
	fmt.Fprintf(os.Stderr, "  udec -schema schema.yaml messages.smsg\n")
	fmt.Fprintf(os.Stderr, "  udec -v -schema schema.yaml messages.smsg\n")
}
