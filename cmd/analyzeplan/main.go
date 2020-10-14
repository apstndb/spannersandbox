package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner/spansql"
	"github.com/apstndb/spannerplanviz/queryplan"
	"google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

type stringSet map[string]struct{}

func (s stringSet) Add(v string) {
	s[v] = struct{}{}
}

func (s stringSet) Union(other stringSet) {
	for v := range other {
		s[v] = struct{}{}
	}
}

func (s stringSet) Has(v string) bool {
	_, ok := s[v]
	return ok
}

func (s stringSet) Slice() []string {
	var slice []string
	for v, _ := range s {
		slice = append(slice, v)
	}
	sort.Strings(slice)
	return slice
}

func (s stringSet) String() string {
	var keys []string
	for k, _ := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Sprint(keys)
}
func newSingleton(v string) stringSet {
	return stringSet{v: struct{}{}}
}

func difference(lhs, rhs stringSet) stringSet {
	result := make(stringSet)
	for s := range lhs {
		if !rhs.Has(s) {
			result.Add(s)
		}
	}
	return result
}

func intersection(lhs, rhs stringSet) stringSet {
	result := make(stringSet)
	for s := range lhs {
		if rhs.Has(s) {
			result.Add(s)
		}
	}
	return result
}

type tree struct {
	typ      OperatorType
	node     *spanner.PlanNode
	children []*tree
	scalars  []*tree
}

func (t *tree) String() string {
	switch t.typ {
	case LeafRelational:
		if t.node.GetDisplayName() == "Scan" {
			scanType := t.node.Metadata.GetFields()["scan_type"].GetStringValue()
			scanTarget := t.node.Metadata.GetFields()["scan_target"].GetStringValue()
			return fmt.Sprintf("%s<%s:%s>", escapeIfNeeded(t.node.GetDisplayName()), scanType, scanTarget)
		}
		return t.node.GetDisplayName()
	case UnaryRelational:
		if len(t.scalars) == 0 {
			return t.children[0].String()
		}
		var scalarsStr []string
		for _, child := range t.scalars {
			scalarsStr = append(scalarsStr, child.String())
		}
		return fmt.Sprintf("%s<Scalar:[%s]>(%s)", escapeIfNeeded(t.node.GetDisplayName()), strings.Join(scalarsStr, ", "), t.children[0].String())
	case SubqueryScalar:
		return t.children[0].String()
	case BinaryRelational:
		var right *tree
		if t.node.GetMetadata().GetFields()["subquery_cluster_node"].GetStringValue() != "" {
			var current *tree = t.children[1]
			for {
				if current.typ == UnaryRelational && len(current.scalars) == 0 {
					current = current.children[0]
					continue
				}
				if current.typ == BinaryRelational {
					currentLeft := current.children[0]
					if currentLeft.node.GetDisplayName() == "Scan" && currentLeft.node.GetMetadata().AsMap()["scan_type"] == "BatchScan" {
						currentRight := current.children[1]
						right = currentRight
						break
					}
				}
				// fallback
				right = t.children[1]
				break
			}
		} else {
			right = t.children[1]
		}
		left := t.children[0]
		return fmt.Sprintf("(%s `%s` %s)", left.String(), t.node.GetDisplayName(), right.String())
	default:
		var childrenStr []string
		for _, child := range t.children {
			childrenStr = append(childrenStr, child.String())
		}
		var scalarsStr []string
		for _, child := range t.scalars {
			scalarsStr = append(scalarsStr, child.String())
		}
		if len(scalarsStr) > 0 {
			childrenStr = append(childrenStr, fmt.Sprintf("Scalar: [%s]", strings.Join(scalarsStr, ", ")))
		}
		return fmt.Sprintf("%s(%s)", escapeIfNeeded(t.node.GetDisplayName()), strings.Join(childrenStr, ", "))
	}
}

func escapeIfNeeded(s string) string {
	if strings.Contains(s, " ") {
		return strconv.Quote(s)
	}
	return s
}
func main() {
	if err := _main(); err != nil {
		log.Fatalln(err)
	}
}
func _main() error {
	var b []byte
	var err error
	ddlFile := flag.String("ddl-file", "", "")
	flag.Parse()

	if *ddlFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	var ddl *spansql.DDL
	{
		b, err := ioutil.ReadFile(*ddlFile)
		if err != nil {
			return err
		}
		ddl, err = spansql.ParseDDL(*ddlFile, string(b))
		if err != nil {
			return err
		}
	}
	if flag.NArg() == 0 {
		b, err = ioutil.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
	} else {
		filename := os.Args[1]
		b, err = ioutil.ReadFile(filename)
		if err != nil {
			return err
		}
	}
	var qp spanner.QueryPlan
	if err := protojson.Unmarshal(b, &qp); err != nil {
		return err
	}
	planNodes := qp.GetPlanNodes()
	fmt.Println(ToTree(planNodes, nil).String())
	paths := calcPaths(planNodes)

	tablesMap := tables(ddl)
	indexesMap := indexes(ddl)

	tableToIndex := make(map[string]stringSet)
	indexToTable := make(map[string]string)
	indexesColumnsMap := make(map[string]stringSet)
	for _, idx := range indexesMap {
		if _, ok := tableToIndex[idx.Table]; !ok {
			tableToIndex[idx.Table] = make(stringSet)
		}
		tableToIndex[idx.Table].Add(idx.Name)
		indexToTable[idx.Name] = idx.Table
		columns := make(stringSet)
		for _, c := range idx.Columns {
			columns.Add(c.Column)
		}
		for _, c := range idx.Storing {
			columns.Add(c)
		}
		for _, c := range tablesMap[idx.Table].PrimaryKey {
			columns.Add(c.Column)
		}
		indexesColumnsMap[idx.Name] = columns
	}

	for _, pn := range planNodes {
		if pn.GetDisplayName() != "Scan" || pn.GetMetadata().AsMap()["scan_type"] != "TableScan" {
			continue
		}
			table := pn.GetMetadata().AsMap()["scan_target"].(string)
			scanColumnsSet := make(stringSet)
			scanVariableMap := make(map[string]string)
			for _, scanChild := range pn.GetChildLinks() {
				scanChildNode := planNodes[scanChild.GetChildIndex()]
				if scanChildNode.GetDisplayName() != "Reference" {
					log.Printf("Unknown children %v", scanChildNode)
					continue
				}
				scanColumnsSet.Add(scanChildNode.GetShortRepresentation().GetDescription())
				scanVariableMap["$" + scanChild.GetVariable()] = scanChildNode.GetShortRepresentation().GetDescription()
			}
			tableScanPath := paths[pn.GetIndex()]
			for _, idxNode := range planNodes {
				if idxNode.GetDisplayName() != "Scan" || idxNode.GetMetadata().AsMap()["scan_type"] != "IndexScan" {
					continue
				}
				idx := idxNode.GetMetadata().AsMap()["scan_target"].(string)
				if idxBaseTable, ok := indexToTable[idx]; idxBaseTable != table {
					if !ok {
						fmt.Println("Unknown index:", idx)
					}
					continue
				}
				indexScanPath := paths[idxNode.GetIndex()]
				fmt.Printf("Possibly back join table %s at %v and index %s at %v at: %d\n", table, tableScanPath, idx, indexScanPath, lowestCommonAncestor(tableScanPath, indexScanPath))
				notFoundSet := difference(scanColumnsSet, indexesColumnsMap[idx])
				fmt.Printf("Possibly because used columns is not contained in the index: %v\n", notFoundSet)
				fmt.Println("  Candidate DDL to avoid back join:", addStoring(indexesMap[idx], notFoundSet.Slice()).SQL())
				if len(tableScanPath) > 2 {
					tableScanParent := planNodes[tableScanPath[len(tableScanPath)-2]]
					if tableScanParent.GetDisplayName() != "FilterScan" {
						continue
					}
					for _, cl := range tableScanParent.GetChildLinks() {
						if cl.GetType() != "Residual Condition" {
							continue
						}
						residualConditionReferencedColumnsSet := make(stringSet)
						for k := range collectReference(planNodes, planNodes[cl.GetChildIndex()]) {
							if _, ok := scanVariableMap[k]; !ok {
								continue
							}
							residualConditionReferencedColumnsSet.Add(scanVariableMap[k])
						}
						filterNotFoundSet := difference(residualConditionReferencedColumnsSet, indexesColumnsMap[idx])
						fmt.Printf("Residual condition at FilterScan(%d) is possibly because used columns is not contained, index: %s, missing columns: %v\n", tableScanParent.GetIndex(), idx, filterNotFoundSet)
						fmt.Println("  Candidate DDL to optimize filter:", addStoring(indexesMap[idx], filterNotFoundSet.Slice()).SQL())
					}
				}
			}
	}
	return nil
}

func addStoring(createIndex *spansql.CreateIndex, columns []string) *spansql.CreateIndex {
	storing := make(stringSet)
	for _, c := range createIndex.Storing {
		storing.Add(c)
	}
	for _, c := range columns {
		storing.Add(c)
	}

	return &spansql.CreateIndex{
		Name:         createIndex.Name,
		Table:        createIndex.Table,
		Columns:      createIndex.Columns,
		Unique:       createIndex.Unique,
		NullFiltered: createIndex.NullFiltered,
		Storing:      storing.Slice(),
		Interleave:   createIndex.Interleave,
		Position:     spansql.Position{},
	}
}

func tables(ddl *spansql.DDL) map[string]*spansql.CreateTable {
	result := make(map[string]*spansql.CreateTable)
	for _, d := range ddl.List {
		switch d := d.(type) {
		case *spansql.CreateTable:
			result[d.Name] = d
		}
	}
	return result
}

func indexes(ddl *spansql.DDL) map[string]*spansql.CreateIndex {
	result := make(map[string]*spansql.CreateIndex)
	for _, d := range ddl.List {
		switch d := d.(type) {
		case *spansql.CreateIndex:
			result[d.Name] = d
		}
	}
	return result
}

type OperatorType int

const (
	Unknown OperatorType = iota
	LeafRelational
	UnaryRelational
	BinaryRelational
	NAryRelational
	SubqueryScalar
	OtherScalar
)

func Type(planNodes []*spanner.PlanNode, node *spanner.PlanNode) OperatorType {
	switch node.GetKind() {
	case spanner.PlanNode_RELATIONAL:
		relCl := RelationalChildLinks(planNodes, node)
		switch {
		case node.GetDisplayName() == "Union All":
			return NAryRelational
		case len(relCl) == 0:
			return LeafRelational
		case len(relCl) == 1:
			return UnaryRelational
		case len(relCl) == 2:
			return BinaryRelational
		}
	case spanner.PlanNode_SCALAR:
		relCl := RelationalChildLinks(planNodes, node)
		switch {
		case len(relCl) == 0:
			return OtherScalar
		case len(relCl) == 1:
			return SubqueryScalar
		case len(relCl) == 2:
			return Unknown
		}
	default:
		return Unknown
	}
	return Unknown
}

func RelationalChildLinks(planNodes []*spanner.PlanNode, node *spanner.PlanNode) []*spanner.PlanNode_ChildLink {
	var result []*spanner.PlanNode_ChildLink
	for _, cl := range node.GetChildLinks() {
		if planNodes[cl.GetChildIndex()].GetKind() == spanner.PlanNode_RELATIONAL {
			result = append(result, cl)
		}
	}
	return result
}

func ToTree(planNodes []*spanner.PlanNode, link *spanner.PlanNode_ChildLink) *tree {
	node := planNodes[link.GetChildIndex()]
	var children []*tree
	var scalars []*tree
	for _, cl := range node.GetChildLinks() {
		child := planNodes[cl.GetChildIndex()]
		if child.GetKind() == spanner.PlanNode_RELATIONAL {
			children = append(children, ToTree(planNodes, cl))
		}
		if cl.GetType() == "Scalar" {
			scalars = append(scalars, ToTree(planNodes, cl))
		}
	}
	return &tree{typ: Type(planNodes, node), node: node, children: children, scalars: scalars}
}

func calcPaths(planNodes []*spanner.PlanNode) map[int32][]int32 {
	result := map[int32][]int32{0: {0}}
	calcPathsImpl(queryplan.New(planNodes), nil, result)
	return result
}

func calcPathsImpl(qp *queryplan.QueryPlan, childLink *spanner.PlanNode_ChildLink, acc map[int32][]int32) {
	node := qp.GetNodeByChildLink(childLink)
	parentAncestor := acc[node.GetIndex()]
	for _, cl := range qp.VisibleChildLinks(node) {
		ancestor := make([]int32, len(parentAncestor)+1)
		copy(ancestor, parentAncestor)
		childIndex := cl.GetChildIndex()
		ancestor[len(parentAncestor)] = childIndex
		acc[childIndex] = ancestor
		calcPathsImpl(qp, cl, acc)
	}
}

func lowestCommonAncestor(lhs []int32, rhs []int32) int32 {
	var result int32
	for i := 0; i < len(lhs) && i < len(rhs) && lhs[i] == rhs[i]; i++ {
		result = lhs[i]
	}
	return result
}

func collectReference(planNodes []*spanner.PlanNode, node *spanner.PlanNode) stringSet {
	switch node.GetDisplayName() {
	case "Reference":
		return newSingleton(node.GetShortRepresentation().GetDescription())
	case "Function":
		result := make(stringSet)
		for _, cl := range node.GetChildLinks() {
			result.Union(collectReference(planNodes, planNodes[cl.GetChildIndex()]))
		}
		return result
	default:
		return nil
	}
}
