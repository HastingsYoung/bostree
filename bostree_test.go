package bostree

import (
	"fmt"
	. "github.com/bostree/bos_node"
	"github.com/bostree/ex_math"
	"testing"
	"time"
)

func TestTreeSanity(t *testing.T) {
	var (
		count uint64 = 0
	)

	tree := Build(func(k1, k2 interface{}) int {
		var (
			f1 = k1.(float64)
			f2 = k2.(float64)
		)
		if f1 > f2 {
			return 1
		}
		if f1 == f2 {
			return 0
		}
		return -1
	})

	for i := 0; i < 100; i++ {
		tree.Insert(float64(i), fmt.Sprintf("p%d", i))
	}
	for node := tree.Select(0); node != nil; node = tree.NxtNode(node) {
		count++
	}

	t.Run("Tree Counting", func(t *testing.T) {
		if count != tree.NodeCount() {
			t.Errorf(
				"Expected %d, but got %d\n",
				count,
				tree.NodeCount(),
			)
		}
	})

	for i := uint64(0); i < tree.NodeCount(); i++ {
		node := tree.Select(i)
		t.Run("Rank & Counting", func(t *testing.T) {
			if node == nil {
				t.Errorf(
					"Expected %d, but got %d\n",
					count,
					tree.NodeCount(),
				)
			}
		})
		t.Run("Ranking", func(t *testing.T) {
			if tree.Rank(node) != i {
				t.Errorf(
					"Expected %f, but got %f\n",
					node.Key,
					float64(tree.Rank(node)),
				)
			}
		})
		t.Run("Lookup", func(t *testing.T) {
			if lookUp := tree.LookUp(node.Key); lookUp != node {
				t.Errorf(
					"Expected %f, but got %f\n",
					node.Key,
					lookUp.Key,
				)
			}
		})
		t.Run("Next Node", func(t *testing.T) {
			if opNode := tree.NxtNode(node); opNode != tree.Select(i+1) {
				t.Errorf(
					"Expected %s, but got %s\n",
					fmt.Sprint(opNode),
					fmt.Sprint(tree.Select(i+1)),
				)
			}
		})
		t.Run("Previous Node", func(t *testing.T) {
			if opNode := tree.PrevNode(node); opNode != tree.Select(i-1) {
				t.Errorf(
					"Expected %s, but got %s\n",
					fmt.Sprint(opNode),
					fmt.Sprint(tree.Select(i-1)),
				)
			}
		})
		t.Run("Parent Connection", func(t *testing.T) {
			if node.HasParent() {
				if node.ParentNode.LeftChildNode != node && node.ParentNode.RightChildNode != node {
					t.Errorf(
						"Expected %s, but got %s/%s\n",
						fmt.Sprint(node),
						fmt.Sprint(node.ParentNode.LeftChildNode),
						fmt.Sprint(node.ParentNode.RightChildNode),
					)
				}
			}
		})

		depth := actualDepth(node)

		t.Run("Depth", func(t *testing.T) {

			if depth != node.Depth {
				t.Errorf(
					"Expected %d, but got %d\n",
					depth,
					node.Depth,
				)
			}
		})

		t.Run("Left Children Count", func(t *testing.T) {

			if node.HasLeftChild() {
				leftCount := actualCount(node.LeftChildNode)
				if leftCount != node.LeftChildCount {
					t.Errorf(
						"Expected %d, but got %d\n",
						leftCount,
						node.LeftChildCount,
					)
				}
			}
		})

		t.Run("Right Children Count", func(t *testing.T) {

			if node.HasRightChild() {
				rightCount := actualCount(node.RightChildNode)
				if rightCount != node.RightChildCount {
					t.Errorf(
						"Expected %d, but got %d\n",
						rightCount,
						node.RightChildCount,
					)
				}
			}
		})

		var (
			leftDepth  = node.LeftDepth()
			rightDepth = node.RightDepth()
		)
		t.Run("Balance", func(t *testing.T) {
			if leftDepth > rightDepth {
				if leftDepth-rightDepth > 1 {
					t.Errorf(
						"Expected <=1, but got %d/%d\n",
						leftDepth,
						rightDepth,
					)
				}
			}
		})
	}
}

func TestProfiling(t *testing.T) {
	tree := Build(func(k1, k2 interface{}) int {
		var (
			f1 = k1.(float64)
			f2 = k2.(float64)
		)
		if f1 > f2 {
			return 1
		}
		if f1 == f2 {
			return 0
		}
		return -1
	})

	var nodeArr []*BOSNode

	for i := 0; i < 1000000; i++ {
		n := tree.Insert(float64(i), fmt.Sprintf("p%d", i))
		if i%100000 == 0 {
			nodeArr = append(nodeArr, n)
		}
	}

	t.Run("Find Margin", func(t *testing.T) {

		stTime := time.Now()
		lookUp := tree.LookUp(float64(0))
		edTime := time.Now()
		if lookUp == nil {
			t.Errorf(
				"Expected: node exists but got nil",
			)
		}
		t.Logf("Find Margin [%f] in 1000000 Records: %dns \n", lookUp.Key, edTime.Sub(stTime).Nanoseconds())
	})

	t.Run("Find Middle", func(t *testing.T) {

		stTime := time.Now()
		lookUp := tree.LookUp(float64(500000))
		edTime := time.Now()
		if lookUp == nil {
			t.Errorf(
				"Expected: node exists but got nil",
			)
		}
		t.Logf("Find Middle [%f] in 1000000 Records: %dns \n", lookUp.Key, edTime.Sub(stTime).Nanoseconds())
	})

	t.Run("Find Rank", func(t *testing.T) {

		var sumTime int64 = 0
		for _, n := range nodeArr {
			stTime := time.Now()
			rank := tree.Rank(n)
			edTime := time.Now()
			t.Logf("Find Rank [%d] in 1000000 Records: %dns \n", rank, edTime.Sub(stTime).Nanoseconds())
			sumTime += edTime.Sub(stTime).Nanoseconds()
		}
		t.Logf("Find Rank Average in 1000000 Records: %dns \n", sumTime/int64(len(nodeArr)))
	})
}

func actualDepth(node *BOSNode) uint64 {
	var (
		leftDepth = func() uint64 {
			if node.HasLeftChild() {
				return actualDepth(node.LeftChildNode) + 1
			}
			return 0
		}()
		rightDepth = func() uint64 {
			if node.HasRightChild() {
				return actualDepth(node.RightChildNode) + 1
			}
			return 0
		}()
	)
	return ex_math.Uint64Max(leftDepth, rightDepth)
}

func actualCount(node *BOSNode) uint64 {
	var (
		leftCount = func() uint64 {
			if node.HasLeftChild() {
				return actualCount(node.LeftChildNode)
			}
			return 0
		}()
		rightCount = func() uint64 {
			if node.HasRightChild() {
				return actualCount(node.RightChildNode)
			}
			return 0
		}()
	)
	return leftCount + rightCount + 1
}
