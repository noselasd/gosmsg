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
	"strings"

	"github.com/noselasd/gosmsg"
)

// schemaFiles is a custom flag type that accumulates multiple schema file/directory paths
type schemaFiles []string

func (s *schemaFiles) String() string {
	return strings.Join(*s, ",")
}

func (s *schemaFiles) Set(value string) error {
	*s = append(*s, value)
	return nil
}

var (
	schemas  schemaFiles
	verbose  = flag.Bool("v", false, "Enable verbose output for raw mode")
	showHelp = flag.Bool("h", false, "Show help message")
)

func main() {
	flag.Var(&schemas, "schema", "YAML schema file or directory (can be specified multiple times)")
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

	// Load schemas if provided
	var decoder *gosmsg.SchemaDecoder
	var loadedSchemas []gosmsg.Schema
	if len(schemas) > 0 {
		var err error
		loadedSchemas, err = loadSchemas(schemas)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading schemas: %v\n", err)
			os.Exit(1)
		}

		decoder, err = gosmsg.NewSchemaDecoder(loadedSchemas)
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

		if decoder != nil {
			// Schema mode
			printWithSchema(msg, decoder)
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

	// Print all tags recursively
	printTags(recordTag.SubTags(), "  ")
}

// printTags recursively prints tags with proper indentation
func printTags(it gosmsg.Iter, indent string) {
	for {
		tag, err := it.NextTag()
		if err == gosmsg.EOS {
			break
		}
		if err != nil {
			fmt.Printf("%sError reading tag: %v\n", indent, err)
			break
		}

		// Check if this is a constructor tag
		if tag.Constructor {
			// Print constructor tag header
			if *verbose {
				if tag.VarLen {
					fmt.Printf("%s0x%04X (constructor, varlen):\n", indent, tag.Tag)
				} else {
					fmt.Printf("%s0x%04X (constructor, length: %3d):\n", indent, tag.Tag, len(tag.Data))
				}
			} else {
				fmt.Printf("%s0x%04X (constructor):\n", indent, tag.Tag)
			}
			// Recursively print subtags with increased indentation
			printTags(tag.SubTags(), indent+"  ")
		} else {
			// Print regular tag with data
			if *verbose {
				fmt.Printf("%s0x%04X (length: %3d):  %s\n", indent, tag.Tag, len(tag.Data), tag.Data)
			} else {
				fmt.Printf("%s0x%04X:  %s\n", indent, tag.Tag, tag.Data)
			}
		}
		if tag.Tag == 0 {
			break // Terminator
		}

	}
}

// loadSchemas loads schemas from a list of file or directory paths
func loadSchemas(paths []string) ([]gosmsg.Schema, error) {
	var schemas []gosmsg.Schema
	seenTags := make(map[uint16]bool)

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("cannot access %s: %w", path, err)
		}

		if info.IsDir() {
			// Load all .yaml and .yml files from directory (non-recursive)
			entries, err := os.ReadDir(path)
			if err != nil {
				return nil, fmt.Errorf("cannot read directory %s: %w", path, err)
			}

			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				name := entry.Name()
				if strings.HasSuffix(name, ".yaml") || strings.HasSuffix(name, ".yml") {
					fullPath := path + string(os.PathSeparator) + name
					schema, err := gosmsg.LoadSchema(fullPath)
					if err != nil {
						return nil, fmt.Errorf("loading schema from %s: %w", fullPath, err)
					}

					// Check for duplicate record tags
					if tagVal, ok := schema.RecordType.Metadata["smsg_tag"].(int); ok {
						tag := uint16(tagVal)
						if seenTags[tag] {
							return nil, fmt.Errorf("duplicate record tag 0x%04X in %s", tag, fullPath)
						}
						seenTags[tag] = true
					}

					schemas = append(schemas, *schema)
				}
			}
		} else {
			// Load single schema file
			schema, err := gosmsg.LoadSchema(path)
			if err != nil {
				return nil, fmt.Errorf("loading schema from %s: %w", path, err)
			}

			// Check for duplicate record tags
			if tagVal, ok := schema.RecordType.Metadata["smsg_tag"].(int); ok {
				tag := uint16(tagVal)
				if seenTags[tag] {
					return nil, fmt.Errorf("duplicate record tag 0x%04X in %s", tag, path)
				}
				seenTags[tag] = true
			}

			schemas = append(schemas, *schema)
		}
	}

	if len(schemas) == 0 {
		return nil, fmt.Errorf("no schemas loaded from provided paths")
	}

	return schemas, nil
}

// printWithSchema prints a message using schema interpretation
func printWithSchema(msg gosmsg.RawSMsg, decoder *gosmsg.SchemaDecoder) {
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
	fmt.Printf("Record: %s (tag: 0x%04X)\n", decoded.RecordType, decoded.RecordTag)

	// Calculate column widths for field names
	maxNameLen := 0
	for name := range decoded.Fields {
		if len(name) > maxNameLen {
			maxNameLen = len(name)
		}
	}

	// Print fields with alignment (sorted by name for consistent output)
	var fieldNames []string
	for name := range decoded.Fields {
		fieldNames = append(fieldNames, name)
	}

	for _, name := range fieldNames {
		value := decoded.Fields[name]
		fmt.Printf("  %-*s:  %v\n", maxNameLen, name, value)
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
