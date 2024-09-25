package main

/*
#include <stdlib.h>

struct result {
    char* res;
    char* err;
};
*/
import "C"
import (
	"unsafe"

	s "github.com/Aiven-Open/karapace/go/protopace/schema"
)

func result(schema string, err error) *C.struct_result {
	res := (*C.struct_result)(C.malloc(C.size_t(unsafe.Sizeof(C.struct_result{}))))
	res.res = C.CString(schema)
	res.err = nil
	if err != nil {
		res.err = C.CString(err.Error())
	}
	return res
}

//export FreeResult
func FreeResult(result *C.struct_result) {
	C.free(unsafe.Pointer(result.res))
	C.free(unsafe.Pointer(result.err))
	C.free(unsafe.Pointer(result))
}

func createSchema(cSchemaName *C.char, cSchema *C.char, cDependencyNames **C.char, cDependencies **C.char, depsLenght C.int) (*s.Schema, error) {
	depArray := unsafe.Slice(cDependencies, depsLenght)
	depNamesArray := unsafe.Slice(cDependencyNames, depsLenght)
	dependencies := []s.Schema{}
	for i, dep := range depArray {
		dependency, err := s.FromString(C.GoString(depNamesArray[i]), C.GoString(dep), []s.Schema{})
		if err != nil {
			return nil, err
		}
		dependencies = append(dependencies, *dependency)
	}

	schema, err := s.FromString(C.GoString(cSchemaName), C.GoString(cSchema), dependencies)
	return schema, err
}

//export FormatSchema
func FormatSchema(cSchemaName *C.char, cSchema *C.char, cDependencyNames **C.char, cDependencies **C.char, depsLenght C.int) *C.struct_result {
	schema, err := createSchema(cSchemaName, cSchema, cDependencyNames, cDependencies, depsLenght)
	if err != nil {
		return result("", err)
	}

	res, err := Format(*schema)
	if err != nil {
		return result("", err)
	}
	return result(res.Schema, err)
}

//export CheckCompatibility
func CheckCompatibility(
	cSchemaName *C.char, cSchema *C.char, cDependencyNames **C.char, cDependencies **C.char, depsLenght C.int,
	cSchemaNamePrev *C.char, cSchemaPrev *C.char, cDependencyNamesPrev **C.char, cDependenciesPrev **C.char, depsLenghtPrev C.int) *C.char {

	schema, err := createSchema(cSchemaName, cSchema, cDependencyNames, cDependencies, depsLenght)
	if err != nil {
		return C.CString(err.Error())
	}
	prevSchema, err := createSchema(cSchemaNamePrev, cSchemaPrev, cDependencyNamesPrev, cDependenciesPrev, depsLenghtPrev)
	if err != nil {
		return C.CString(err.Error())
	}

	err = Check(*schema, *prevSchema)
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

func main() {}
