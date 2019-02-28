package bostree

import (
	"errors"
	"fmt"
	. "github.com/bostree/bos_node"
	"github.com/bostree/ex_math"
)

type BOSTree struct {
	RootNode *BOSNode
	CmpFunc  func(k1, k2 interface{}) int
}

// helper functions
func BOSTreeBalance(node *BOSNode) int64 {
	var (
		leftDepth  uint64
		rightDepth uint64
	)
	if node.HasLeftChild() {
		leftDepth = node.LeftChildNode.Depth + 1
	} else {
		leftDepth = 0
	}
	if node.HasRightChild() {
		rightDepth = node.RightChildNode.Depth + 1
	} else {
		rightDepth = 0
	}
	if rightDepth > leftDepth {
		return int64(rightDepth) - int64(leftDepth)
	} else {
		return -int64((leftDepth - rightDepth))
	}
}

// Rotate right:
//
//       P                     L
//   L        R     -->    c1      P
// c1 c2                        c2     R
//
func BOSTreeRotateRight(tree *BOSTree, p *BOSNode) *BOSNode {

	var (
		ln *BOSNode = p.LeftChildNode
		// rn *BOSNode = p.RightChildNode
	)
	if p.HasParent() {
		if p.IsParentLeftChild() {
			p.ParentNode.LeftChildNode = ln
		} else {
			p.ParentNode.RightChildNode = ln
		}
	} else {
		tree.RootNode = ln
	}

	ln.ParentNode = p.ParentNode

	p.LeftChildNode = ln.RightChildNode
	p.LeftChildCount = ln.RightChildCount

	if p.HasLeftChild() {
		p.LeftChildNode.ParentNode = p
	}

	p.Depth = ex_math.Uint64Max(func() uint64 {
		if p.HasLeftChild() {
			return p.LeftChildNode.Depth + 1
		}
		return 0
	}(), func() uint64 {
		if p.HasRightChild() {
			return p.RightChildNode.Depth + 1
		}
		return 0
	}())

	p.ParentNode = ln
	ln.RightChildNode = p
	ln.RightChildCount = p.LeftChildCount + p.RightChildCount + 1
	ln.Depth = ex_math.Uint64Max(func() uint64 {
		if ln.HasLeftChild() {
			return ln.LeftChildNode.Depth + 1
		}
		return 0
	}(), func() uint64 {
		if ln.HasRightChild() {
			return ln.RightChildNode.Depth + 1
		}
		return 0
	}())

	return ln
}

// Rotate left:
//
//      P                     R
//  L        R     -->    P      c2
//         c1 c2        L  c1
func BOSTreeRotateLeft(tree *BOSTree, p *BOSNode) *BOSNode {
	var (
		// ln *BOSNode = p.LeftChildNode
		rn *BOSNode = p.RightChildNode
	)
	if p.HasParent() {
		if p.IsParentLeftChild() {
			p.ParentNode.LeftChildNode = rn
		} else {
			p.ParentNode.RightChildNode = rn
		}
	} else {
		tree.RootNode = rn
	}

	rn.ParentNode = p.ParentNode

	p.RightChildNode = rn.LeftChildNode
	p.RightChildCount = rn.LeftChildCount

	if p.HasRightChild() {
		p.RightChildNode.ParentNode = p
	}

	p.Depth = ex_math.Uint64Max(func() uint64 {
		if p.HasLeftChild() {
			return p.LeftChildNode.Depth + 1
		}
		return 0
	}(), func() uint64 {
		if p.HasRightChild() {
			return p.RightChildNode.Depth + 1
		}
		return 0
	}())

	p.ParentNode = rn
	rn.LeftChildNode = p
	rn.LeftChildCount = p.LeftChildCount + p.RightChildCount + 1
	rn.Depth = ex_math.Uint64Max(func() uint64 {
		if rn.HasLeftChild() {
			return rn.LeftChildNode.Depth + 1
		}
		return 0
	}(), func() uint64 {
		if rn.HasRightChild() {
			return rn.RightChildNode.Depth + 1
		}
		return 0
	}())

	return rn
}

func (tree *BOSTree) Insert(key, val interface{}) *BOSNode {
	var (
		node       *BOSNode = tree.RootNode
		parentNode *BOSNode = nil
		newNode    *BOSNode = NewNode()
	)

	newNode.Key = key
	newNode.Val = val

	for node != nil {
		parentNode = node
		cmp := tree.CmpFunc(key, node.Key)
		if cmp < 0 {
			// go into left subtree
			node.LeftChildCount++
			node = node.LeftChildNode
		} else {
			// go into right subtree
			node.RightChildCount++
			node = node.RightChildNode
		}
	}

	if parentNode == nil {
		// this is the first node
		tree.RootNode = newNode
		return newNode
	}

	parentNode.Depth++

	cmp := tree.CmpFunc(parentNode.Key, newNode.Key)

	if cmp < 0 {
		parentNode.RightChildNode = newNode
		newNode.ParentNode = parentNode
	} else {
		parentNode.LeftChildNode = newNode
		newNode.ParentNode = parentNode
	}
	for parentNode.HasParent() {
		parentNode = parentNode.ParentNode
		var (
			newLeftDepth = func() uint64 {
				if parentNode.HasLeftChild() {
					return parentNode.LeftChildNode.Depth + 1
				}
				return 0
			}()
			newRightDepth = func() uint64 {
				if parentNode.HasRightChild() {
					return parentNode.RightChildNode.Depth + 1
				}
				return 0
			}()
			maxDepth = ex_math.Uint64Max(newLeftDepth, newRightDepth)
		)

		if parentNode.Depth != maxDepth {
			parentNode.Depth = maxDepth
		} else {
			break
		}

		if newLeftDepth-2 == newRightDepth {
			if BOSTreeBalance(parentNode.LeftChildNode) > 0 {
				BOSTreeRotateLeft(tree, parentNode.LeftChildNode)
			}
			parentNode = BOSTreeRotateRight(tree, parentNode)
		} else if newLeftDepth+2 == newRightDepth {
			if BOSTreeBalance(parentNode.RightChildNode) < 0 {
				BOSTreeRotateRight(tree, parentNode.RightChildNode)
			}
			parentNode = BOSTreeRotateLeft(tree, parentNode)
		}
	}

	return newNode
}

func (tree *BOSTree) Remove(node *BOSNode) {
	var (
		bubbleUp *BOSNode
	)

	// If this node has children on both sides, bubble one of it upwards
	// and rotate within the subtrees.
	if node.HasLeftChild() && node.HasRightChild() {
		var (
			candidate,
			lostChild *BOSNode
		)

		if node.LeftChildNode.Depth >= node.RightChildNode.Depth {
			// Left branch is deeper than right branch, might be a good idea to
			// bubble from this side to maintain the AVL property with increased
			// likelihood.
			node.LeftChildCount--
			candidate = node.LeftChildNode
			for candidate.HasRightChild() {
				candidate.RightChildCount--
				candidate = candidate.RightChildNode
			}
			lostChild = candidate.LeftChildNode
		} else {
			node.RightChildCount--
			candidate = node.RightChildNode
			for candidate.HasLeftChild() {
				candidate.LeftChildCount--
				candidate = candidate.LeftChildNode
			}
			lostChild = candidate.RightChildNode
		}

		bubbleStart := candidate.ParentNode

		if candidate.IsParentLeftChild() {
			bubbleStart.LeftChildNode = lostChild
		} else {
			bubbleStart.RightChildNode = lostChild
		}

		if lostChild != nil {
			lostChild.ParentNode = bubbleStart
		}

		// We will later rebalance upwards from bubbleStart up to candidate.
		// But first, anchor candidate into the place where "node" used to be.
		if node.HasParent() {
			if node.IsParentLeftChild() {
				node.ParentNode.LeftChildNode = candidate
			} else {
				node.ParentNode.RightChildNode = candidate
			}
		} else {
			tree.RootNode = candidate
		}

		// Node transplant
		candidate.ParentNode = node.ParentNode
		candidate.LeftChildNode = node.LeftChildNode
		candidate.LeftChildCount = node.LeftChildCount
		candidate.RightChildNode = node.RightChildNode
		candidate.RightChildCount = node.RightChildCount

		if candidate.HasLeftChild() {
			candidate.LeftChildNode.ParentNode = candidate
		}
		if candidate.HasRightChild() {
			candidate.RightChildNode.ParentNode = candidate
		}

		// From here on, node is out of the game.
		// Rebalance up to candidate.

		if bubbleStart != node {
			for bubbleStart != candidate {
				bubbleStart.Depth = ex_math.Uint64Max(
					func() uint64 {
						if bubbleStart.HasLeftChild() {
							return bubbleStart.LeftChildNode.Depth + 1
						}
						return 0
					}(),
					func() uint64 {
						if bubbleStart.HasRightChild() {
							return bubbleStart.RightChildNode.Depth + 1
						}
						return 0
					}(),
				)
				balance := BOSTreeBalance(bubbleStart)
				if balance > 1 {
					// Rotate left. Check for right-left case before.
					if BOSTreeBalance(bubbleStart.RightChildNode) < 0 {
						BOSTreeRotateRight(tree, bubbleStart.RightChildNode)
					}
					bubbleStart = BOSTreeRotateLeft(tree, bubbleStart)
				} else if balance < -1 {
					if BOSTreeBalance(bubbleStart.LeftChildNode) > 0 {
						BOSTreeRotateLeft(tree, bubbleStart.LeftChildNode)
					}
					bubbleStart = BOSTreeRotateRight(tree, bubbleStart)
				}
				bubbleStart = bubbleStart.ParentNode
			}
		}

		candidate.Depth = ex_math.Uint64Max(
			func() uint64 {
				if candidate.HasLeftChild() {
					return candidate.LeftChildNode.Depth + 1
				}
				return 0
			}(),
			func() uint64 {
				if candidate.HasRightChild() {
					return candidate.RightChildNode.Depth + 1
				}
				return 0
			}())

		bubbleUp = candidate.ParentNode

		if bubbleUp != nil {
			if candidate.IsParentLeftChild() {
				bubbleUp.LeftChildCount--
			} else {
				bubbleUp.RightChildCount--
			}
		}
	} else {
		// This node has children on only one side
		if !node.HasParent() {
			if node.HasLeftChild() {
				tree.RootNode = node.LeftChildNode
				node.LeftChildNode.ParentNode = nil
			} else {
				tree.RootNode = node.RightChildNode
				if node.HasRightChild() {
					node.RightChildNode.ParentNode = nil
				}
			}

			bubbleUp = nil
		} else {
			var (
				candidate      *BOSNode = node.LeftChildNode
				candidateCount uint64   = node.LeftChildCount
			)

			if node.HasRightChild() {
				candidate = node.RightChildNode
				candidateCount = node.RightChildCount
			}

			if node.IsParentLeftChild() {
				node.ParentNode.LeftChildNode = candidate
				node.ParentNode.LeftChildCount = candidateCount
			} else {
				node.ParentNode.RightChildNode = candidate
				node.ParentNode.RightChildCount = candidateCount
			}

			if candidate != nil {
				candidate.ParentNode = node.ParentNode
			}

			bubbleUp = node.ParentNode
		}
	}

	var bubbleUpFinished = false

	for bubbleUp != nil {
		if !bubbleUpFinished {
			var (
				leftDepth    = bubbleUp.LeftDepth()
				rightDepth   = bubbleUp.RightDepth()
				newDepth     = ex_math.Uint64Max(leftDepth, rightDepth)
				depthChanged = newDepth != bubbleUp.Depth
			)

			bubbleUp.Depth = newDepth

			// Rebalance bubble_up
			// Not necessary for the first node, but calling BOSTreeBalance once
			// isn't that much overhead.
			balance := BOSTreeBalance(bubbleUp)

			if balance < -1 {
				if BOSTreeBalance(bubbleUp.LeftChildNode) > 0 {
					BOSTreeRotateLeft(tree, bubbleUp.LeftChildNode)
				}
				bubbleUp = BOSTreeRotateRight(tree, bubbleUp)
			} else if balance > 1 {
				if BOSTreeBalance(bubbleUp.RightChildNode) < 0 {
					BOSTreeRotateRight(tree, bubbleUp.RightChildNode)
				}
				bubbleUp = BOSTreeRotateLeft(tree, bubbleUp)
			} else {
				if !depthChanged {
					bubbleUpFinished = true
				}
			}
		}

		if bubbleUp.HasParent() {
			if bubbleUp.IsParentLeftChild() {
				bubbleUp.ParentNode.LeftChildCount--
			} else {
				bubbleUp.ParentNode.RightChildCount--
			}
		}
		bubbleUp = bubbleUp.ParentNode
	}
}

func (tree *BOSTree) LookUp(key interface{}) *BOSNode {
	var (
		node *BOSNode = tree.RootNode
	)

	for node != nil {
		cmp := tree.CmpFunc(key, node.Key)
		if cmp == 0 {
			break
		} else if cmp < 0 {
			node = node.LeftChildNode
		} else {
			node = node.RightChildNode
		}
	}
	return node
}

func (tree *BOSTree) Select(index uint64) *BOSNode {
	var (
		node *BOSNode = tree.RootNode
	)
	for node != nil {
		if node.LeftChildCount <= index {
			index -= node.LeftChildCount
			if index == 0 {
				return node
			}
			index--
			node = node.RightChildNode
		} else {
			node = node.LeftChildNode
		}
	}
	return node
}

func (tree *BOSTree) Rank(node *BOSNode) uint64 {
	var (
		counter = node.LeftChildCount
	)
	for node != nil {
		if node.HasParent() && node.IsParentRightChild() {
			counter += 1 + node.ParentNode.LeftChildCount
		}
		node = node.ParentNode
	}
	return counter
}

func (tree *BOSTree) NxtNode(node *BOSNode) *BOSNode {
	if node.HasRightChild() {
		node = node.RightChildNode
		for node.HasLeftChild() {
			node = node.LeftChildNode
		}
		return node
	} else if node.HasParent() {
		for node.HasParent() && node.IsParentRightChild() {
			node = node.ParentNode
		}
		return node.ParentNode
	}
	return nil
}

func (tree *BOSTree) PrevNode(node *BOSNode) *BOSNode {
	if node.HasLeftChild() {
		node = node.LeftChildNode
		for node.HasRightChild() {
			node = node.RightChildNode
		}
		return node
	} else if node.HasParent() {
		for node.HasParent() && node.IsParentLeftChild() {
			node = node.ParentNode
		}
		return node.ParentNode
	}
	return nil
}

func (tree *BOSTree) PrevValue(key interface{}) (interface{}, error) {
	var (
		node       = tree.LookUp(key)
		err  error = nil
	)
	if node == nil {
		err = errors.New(fmt.Sprintf("Node not found for key: %s", key))
		return nil, err
	}
	preNode := tree.PrevNode(node)
	if preNode == nil {
		return nil, err
	}
	return preNode.Val, err
}

func (tree *BOSTree) NxtValue(key interface{}) (interface{}, error) {
	var (
		node       = tree.LookUp(key)
		err  error = nil
	)
	if node == nil {
		err = errors.New(fmt.Sprintf("Node not found for key: %s", key))
		return nil, err
	}
	nxtNode := tree.NxtNode(node)
	if nxtNode == nil {
		return nil, err
	}
	return nxtNode.Val, err
}

func (tree *BOSTree) NodeCount() uint64 {
	if tree.RootNode != nil {
		return tree.RootNode.LeftChildCount + tree.RootNode.RightChildCount + 1
	}
	return 0
}

func Build(cmp_func func(k1, k2 interface{}) int) *BOSTree {
	var tree = new(BOSTree)
	tree.CmpFunc = cmp_func
	return tree
}

func PrintTree(node *BOSNode) {
	fmt.Printf(
		"%s(%f) [Left: %d/Right: %d/Depth: %d]\n",
		node.Val,
		node.Key,
		node.LeftChildCount,
		node.RightChildCount,
		node.Depth,
	)

	if node.HasParent() {
		fmt.Printf("PR: -> %s(%f) \n", node.ParentNode.Val, node.ParentNode.Key)
	} else {
		fmt.Printf("PR: -> nil \n")
	}

	fmt.Printf("\t| \n")
	fmt.Printf("  <%s(%f)> \n", node.Val, node.Key)
	fmt.Printf("\t| \n")

	if node.HasLeftChild() {
		fmt.Printf("\tLF: -> %s(%f) \n", node.LeftChildNode.Val, node.LeftChildNode.Key)
	} else {
		fmt.Printf("\tLF: -> nil \n")
	}

	if node.HasRightChild() {
		fmt.Printf("\tRT: -> %s(%f) \n", node.RightChildNode.Val, node.RightChildNode.Key)
	} else {
		fmt.Printf("\tRT: -> nil \n")
	}
	fmt.Println()

	if node.HasLeftChild() {
		PrintTree(node.LeftChildNode)
	}
	if node.HasRightChild() {
		PrintTree(node.RightChildNode)
	}
}
