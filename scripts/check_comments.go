//go:build ignore

package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	write := flag.Bool("write", false, "remove comments outside public methods and Go directives")
	flag.Parse()
	invalid, removed := 0, 0
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" || entry.Name() == "bin" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, path, data, parser.ParseComments)
		if err != nil {
			return err
		}
		allowed := map[*ast.CommentGroup]bool{}
		if !strings.HasSuffix(path, "_test.go") {
			for _, decl := range file.Decls {
				if method, ok := decl.(*ast.FuncDecl); ok && method.Recv != nil && ast.IsExported(method.Name.Name) {
					allowed[method.Doc] = true
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				if iface, ok := node.(*ast.InterfaceType); ok {
					for _, method := range iface.Methods.List {
						if _, ok := method.Type.(*ast.FuncType); ok {
							for _, name := range method.Names {
								if ast.IsExported(name.Name) {
									allowed[method.Doc] = true
								}
							}
						}
					}
				}
				return true
			})
		}
		changed := false
		for _, group := range file.Comments {
			for _, comment := range group.List {
				if allowed[group] || strings.HasPrefix(comment.Text, "//go:") {
					continue
				}
				if !*write {
					fmt.Printf("%s: comment outside public method\n", positions.Position(comment.Pos()))
					invalid++
					continue
				}
				start, end := positions.Position(comment.Pos()).Offset, positions.Position(comment.End()).Offset
				for i := start; i < end; i++ {
					if data[i] != '\n' && data[i] != '\r' {
						data[i] = ' '
					}
				}
				removed++
				changed = true
			}
		}
		if changed {
			formatted, err := format.Source(data)
			if err != nil {
				return err
			}
			if err := os.WriteFile(path, formatted, 0644); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if invalid > 0 {
		os.Exit(1)
	}
	if *write {
		fmt.Printf("Removed %d comments; public method documentation and Go directives retained.\n", removed)
	} else {
		fmt.Println("Comment policy OK.")
	}
}
