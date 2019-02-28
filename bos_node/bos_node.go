package bos_node

type BOSNode struct {
	LeftChildCount  uint64
	RightChildCount uint64
	Depth           uint64
	LeftChildNode   *BOSNode
	RightChildNode  *BOSNode
	ParentNode      *BOSNode
	Key             interface{}
	Val             interface{}
}

func (n *BOSNode) HasLeftChild() bool {
	if n.LeftChildNode != nil {
		return true
	}
	return false
}

func (n *BOSNode) HasRightChild() bool {
	if n.RightChildNode != nil {
		return true
	}
	return false
}

func (n *BOSNode) HasParent() bool {
	if n.ParentNode != nil {
		return true
	}
	return false
}

func (n *BOSNode) IsParentLeftChild() bool {
	if !n.HasParent() {
		return false
	}
	if n == n.ParentNode.LeftChildNode {
		return true
	}
	return false
}

func (n *BOSNode) IsParentRightChild() bool {
	if !n.HasParent() {
		return false
	}
	if n == n.ParentNode.RightChildNode {
		return true
	}
	return false
}

func (n *BOSNode) LeftDepth() uint64 {
	if n.HasLeftChild() {
		return n.LeftChildNode.Depth + 1
	}
	return 0
}

func (n *BOSNode) RightDepth() uint64 {
	if n.HasRightChild() {
		return n.RightChildNode.Depth + 1
	}
	return 0
}

func (n *BOSNode) LeftChildDepth() uint64 {
	if n.HasLeftChild() {
		return n.LeftChildNode.Depth
	}
	return 0
}

func (n *BOSNode) RightChildDepth() uint64 {
	if n.HasRightChild() {
		return n.RightChildNode.Depth
	}
	return 0
}

func NewNode() *BOSNode {
	return new(BOSNode)
}
